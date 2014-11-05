require 'active_support/core_ext/hash'

module Masamune::Schema
  class Registry
    include Masamune::HasEnvironment

    class HasMap < Delegator
      attr_accessor :maps

      def initialize(delegate)
        @delegate = delegate
        @maps = {}
      end

      def __getobj__
        @delegate
      end

      def __setobj__(obj)
        @delegate = obj
      end

      def map(options = {})
        self.maps[options[:to]]
      end
    end

    attr_accessor :dimensions
    attr_accessor :facts
    attr_accessor :files
    attr_accessor :events

    def initialize(environment)
      self.environment = environment

      @dimensions = {}.with_indifferent_access
      @facts      = {}.with_indifferent_access
      @files      = {}.with_indifferent_access
      @events     = {}.with_indifferent_access
      @references = {}.with_indifferent_access
      @options    = Hash.new { |h,k| h[k] = [] }
      @extra      = []
    end

    def schema(options = {}, &block)
      @options.merge!(options)
      instance_eval &block
    end

    def dimension(id, options = {}, &block)
      prev_options = @options.dup
      yield if block_given?
      self.dimensions[id] ||= Masamune::Schema::Dimension.new(options.merge(@options).merge(id: id))
      @references[id] ||= Masamune::Schema::TableReference.new(dimensions[id])
    ensure
      @options = prev_options
    end

    def column(id, options = {}, &block)
      prev_options = @options.dup
      @options[:columns] << dereference_column(id, options)
    end

    def references(id, options = {})
      reference = Masamune::Schema::TableReference.new(dimensions[id], options)
      @references[reference.id] = reference
      @options[:references] << reference
    end

    def row(options)
      attributes = options.delete(:attributes) || {}
      attributes[:values] = options
      @options[:rows] << Masamune::Schema::Row.new(attributes)
    end

    def fact(id, options = {}, &block)
      prev_options = @options.dup
      yield if block_given?
      self.facts[id] ||= Masamune::Schema::Fact.new(options.merge(@options).merge(id: id))
    ensure
      @options = prev_options
    end

    def measure(id, options = {}, &block)
      @options[:columns] << Masamune::Schema::Column.new(options.merge(id: id))
    end

    def file(id, options = {}, &block)
      prev_options = @options.dup
      yield if block_given?
      self.files[id] = HasMap.new Masamune::Schema::File.new(options.merge(@options).merge(id: id))
    ensure
      @options = prev_options
    end

    def event(id, options = {}, &block)
      prev_options = @options.dup
      yield if block_given?
      self.events[id] = HasMap.new Masamune::Schema::Event.new(options.merge(@options).merge(id: id))
    ensure
      @options = prev_options
    end

    def attribute(id, options = {}, &block)
      @options[:attributes] << Masamune::Schema::Event::Attribute.new(options.merge(id: id))
    end

    def map(options = {}, &block)
      raise ArgumentError, "invalid map, from: is missing" unless options.is_a?(Hash)
      prev_options = @options.dup
      from, to = options.delete(:from), options.delete(:to)
      raise ArgumentError, "invalid map, from: is missing" unless from && from.try(:id)
      raise ArgumentError, "invalid map from: '#{from.id}', to: is missing" unless to
      @options[:fields] = {}.with_indifferent_access
      yield if block_given?
      from.maps[to] ||= Masamune::Schema::Map.new(options.merge(@options).merge(source: from, target: to))
    ensure
      @options = prev_options
    end

    def field(id, value = nil, &block)
      @options[:fields][id] = value
      @options[:fields][id] ||= block.to_proc if block_given?
      @options[:fields][id] ||= id
    end

    def maps(options = {})
      self.maps[options[:from]][options[:to]]
    end

    def load(file)
      if file =~ /\.rb\Z/
        instance_eval(::File.read(file))
      else
        @extra << ::File.read(file)
      end
    end

     # TODO construct a partial ordering of dimensions by reference
    def as_psql
      output = []
      dimensions.each do |id, dimension|
        logger.debug("#{id}\n" + dimension.as_psql) if dimension.debug
        output << dimension.as_psql
      end
      facts.each do |id, fact|
        logger.debug("#{id}\n" + fact.as_psql) if fact.debug
        output << fact.as_psql
      end
      @extra.each do |extra|
        output << extra
      end
      output.join("\n")
    end

    def to_psql_file
      Tempfile.new('masamune').tap do |file|
        file.write(as_psql)
        file.close
      end.path
    end

    def as_hql
      output = []
      events.each do |id, event|
        t = Masamune::Transform::DefineEventView.new(nil, event)
        logger.debug("#{id}\n" + t.as_hql) if event.debug
        output << t.as_hql
      end
      output.join("\n")
    end

    def to_hql_file
      Tempfile.new('masamune').tap do |file|
        file.write(as_hql)
        file.close
      end.path
    end

    def dereference_column(id, options = {})
      column_id, reference_id = id.split(/\./).reverse
      column_options = options.dup
      column_options.merge!(id: column_id)

      if reference = @references[reference_id]
        column_options.merge!(reference: reference)
      else
        raise ArgumentError, "dimension #{reference_id} not defined"
      end if reference_id

      Masamune::Schema::Column.new(column_options)
    end
  end
end
