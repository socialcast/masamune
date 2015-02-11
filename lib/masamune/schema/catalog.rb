require 'active_support/core_ext/hash'

module Masamune::Schema
  class Catalog
    include Masamune::HasEnvironment

    SUPPORTED_STORES = [:postgres, :hive, :files]

    class HasMap < SimpleDelegator
      attr_accessor :maps

      def initialize(*args)
        super
        @maps = {}
      end

      def map(options = {})
        self.maps[options[:to]]
      end
    end

    class Context < SimpleDelegator
      attr_accessor :options

      def initialize(store, options = {})
        super store
        @store = store
        @options  = Hash.new { |h,k| h[k] = [] }
        @options.merge!(store: @store)
        @options.merge!(options)
      end

      def push(options = {})
        @prev_options = @options.dup
        @options.merge!(options)
      end

      def pop
        @options = @prev_options
      end
    end

    def initialize(environment)
      self.environment = environment
      @catalog = Hash.new { |h,k| h[k] = Masamune::Schema::Store.new(k) }
      @context = nil
    end

    def clear!
      @catalog.clear
    end

    def schema(*args, &block)
      options = args.last.is_a?(Hash) ? args.pop : {}
      raise ArgumentError, 'data store arguments required' unless args.any?
      stores = args.map(&:to_sym)
      stores.each do |id|
        raise ArgumentError, "unknown data store '#{id}'" unless valid_store?(id)
        begin
          @context = Context.new(@catalog[id], options)
          instance_eval &block
        ensure
          @context = nil
        end
      end
    end

    SUPPORTED_STORES.each do |store|
      define_method(store) do
        @catalog[store]
      end
    end

    def [](store_id)
      raise ArgumentError, "unknown data store '#{store_id}'" unless valid_store?(store_id)
      @catalog[store_id.to_sym]
    end

    def table(id, options = {}, &block)
      @context.push(options)
      yield if block_given?
      @context.tables[id] ||= Masamune::Schema::Table.new(@context.options.merge(id: id))
      @context.references[id] ||= Masamune::Schema::TableReference.new(@context.tables[id])
    ensure
      @context.pop
    end

    def dimension(id, options = {}, &block)
      @context.push(options)
      yield if block_given?
      @context.dimensions[id] ||= Masamune::Schema::Dimension.new(@context.options.merge(id: id))
      @context.references[id] ||= Masamune::Schema::TableReference.new(@context.dimensions[id])
    ensure
      @context.pop
    end

    def column(id, options = {}, &block)
      @context.options[:columns] << dereference_column(id, options)
    end

    # FIXME: references should not be ambiguous, e.g. references :user, should be references :user_dimension
    def references(id, options = {})
      table = @context.tables[id] || @context.dimensions[id]
      reference = Masamune::Schema::TableReference.new(table, options.reverse_merge(denormalize: table.implicit))
      @context.references[reference.id] = reference
      @context.options[:references] << reference
    end

    def row(options)
      attributes = options.delete(:attributes) || {}
      attributes[:values] = options
      @context.options[:rows] << Masamune::Schema::Row.new(attributes)
    end

    def fact(id, options = {}, &block)
      @context.push(options)
      grain = Array.wrap(options.delete(:grain) || [])
      fact_attributes(grain).each do |attributes|
        yield if block_given?
        table = Masamune::Schema::Fact.new(@context.options.merge(id: id).merge(attributes))
        @context.facts[table.id] ||= table
      end
    ensure
      @context.pop
    end

    def partition(id, options = {}, &block)
      @context.options[:columns] << Masamune::Schema::Column.new(options.merge(id: id, partition: true))
    end

    def measure(id, options = {}, &block)
      @context.options[:columns] << Masamune::Schema::Column.new(options.merge(id: id, measure: true))
    end

    def file(id, options = {}, &block)
      @context.push(options)
      yield if block_given?
      @context.files[id] = HasMap.new Masamune::Schema::File.new(@context.options.merge(id: id))
    ensure
      @context.pop
    end

    def event(id, options = {}, &block)
      @context.push(options)
      yield if block_given?
      @context.events[id] = HasMap.new Masamune::Schema::Event.new(@context.options.merge(id: id))
    ensure
      @context.pop
    end

    def attribute(id, options = {}, &block)
      @context.options[:attributes] << Masamune::Schema::Event::Attribute.new(options.merge(id: id))
    end

    def map(options = {}, &block)
      raise ArgumentError, "invalid map, from: is missing" unless options.is_a?(Hash)
      from, to = options.delete(:from), options.delete(:to)
      raise ArgumentError, "invalid map, from: is missing" unless from && from.try(:id)
      raise ArgumentError, "invalid map from: '#{from.id}', to: is missing" unless to
      @context.push(options)
      @context.options[:fields] = {}.with_indifferent_access
      yield if block_given?
      from.maps[to] ||= Masamune::Schema::Map.new(@context.options.merge(source: from, target: to))
    ensure
      @context.pop
    end

    def field(id, value = nil, &block)
      @context.options[:fields][id] = value
      @context.options[:fields][id] ||= block.to_proc if block_given?
      @context.options[:fields][id] ||= id
    end

    def load(file)
      case file
      when /\.rb\Z/
        instance_eval(::File.read(file), file)
      when /\.psql\Z/
        @catalog[:postgres].extra << file
      when /\.hql\Z/
        @catalog[:hive].extra << file
      end
    end

    private

    def dereference_column(id, options = {})
      store_id = id.split(/\./).reverse.last
      context = store_id && valid_store?(store_id) ? @catalog[store_id.to_sym] : @context
      context.dereference_column(*id, options)
    end

    def valid_store?(store)
      SUPPORTED_STORES.include?(store.to_sym)
    end

    def fact_attributes(grain = [])
      return [{}] unless grain.any?
      grain.map { |x| { grain: x } }
    end
  end
end
