module Masamune::Schema
  class Registry
    include Masamune::HasEnvironment

    attr_accessor :dimensions
    attr_accessor :facts
    attr_accessor :files
    attr_accessor :maps

    def initialize(environment)
      self.environment = environment

      @dimensions = {}
      @facts      = {}
      @files      = {}
      @maps       = {}
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
      self.dimensions[id.to_sym] ||= Masamune::Schema::Dimension.new(options.merge(@options).merge(id: id))
    ensure
      @options = prev_options
    end

    def column(id, options = {}, &block)
      column_id, column_reference = dereference_column(id)
      @options[:columns] << Masamune::Schema::Column.new(options.merge(id: column_id, reference: column_reference))
    end

    def references(id, options = {})
      @options[:references] << dimensions[id.to_sym].dup.tap do |dimension|
        dimension.label = options[:label]
      end
    end

    def row(options)
      attributes = options.delete(:attributes) || {}
      attributes[:values] = options
      @options[:rows] << Masamune::Schema::Row.new(attributes)
    end

    def fact(id, options = {}, &block)
      prev_options = @options.dup
      yield if block_given?
      self.facts[id.to_sym] ||= Masamune::Schema::Fact.new(options.merge(@options).merge(id: id))
    ensure
      @options = prev_options
    end

    def measure(id, options = {}, &block)
      @options[:columns] << Masamune::Schema::Column.new(options.merge(id: id))
    end

    def file(id, options = {}, &block)
      prev_options = @options.dup
      yield if block_given?
      self.files[id.to_sym] = Masamune::Schema::File.new(options.merge(@options).merge(id: id))
    ensure
      @options = prev_options
    end

    def map(id, options = {}, &block)
      prev_options = @options.dup
      @options[:fields] = {}
      yield if block_given?
      self.maps[id.to_sym] ||= Masamune::Schema::Map.new(options.merge(@options).merge(id: id))
    ensure
      @options = prev_options
    end

    def field(id, value = nil, &block)
      @options[:fields][id.to_sym] = value
      @options[:fields][id.to_sym] ||= block.to_proc if block_given?
      @options[:fields][id.to_sym] ||= id
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

    private

    def dereference_column(id)
      if id =~ /\./
        reference_id, column_id = id.to_s.split('.')
        if dimension = dimensions[reference_id.to_sym]
          [column_id.to_sym, dimension]
        else
          raise ArgumentError, "dimension #{reference_id} not defined"
        end
      else
        [id.to_sym, nil]
      end
    end
  end
end
