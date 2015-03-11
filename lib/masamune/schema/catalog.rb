#  The MIT License (MIT)
#
#  Copyright (c) 2014-2015, VMware, Inc. All Rights Reserved.
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in
#  all copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
#  THE SOFTWARE.

require 'active_support/core_ext/hash'

require 'masamune/schema/store'

module Masamune::Schema
  class Catalog
    include Masamune::HasEnvironment

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

    class HasFormat < SimpleDelegator
      def initialize(store, options = {})
        super store
        @options = options
      end

      def format
        @options.key?(:format) ? @options[:format] : __getobj__.format
      end

      def headers
        @options.key?(:headers) ? @options[:headers] : __getobj__.headers
      end
    end

    class Context < SimpleDelegator
      attr_accessor :options

      def initialize(store, options = {})
        super store
        @store    = store
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
      @stores   = Hash.new { |h,k| h[k] = Masamune::Schema::Store.new(type: k) }
      @context  = nil
    end

    def clear!
      @stores.clear
    end

    def schema(*args, &block)
      options = args.last.is_a?(Hash) ? args.pop : {}
      raise ArgumentError, 'schema store arguments required' unless args.any?
      stores = args.map(&:to_sym)
      stores.each do |id|
        begin
          @context = Context.new(@stores[id], options)
          instance_eval &block
        ensure
          @context = nil
        end
      end
    end

    Masamune::Schema::Store.types.each do |store|
      define_method(store) do
        @stores[store]
      end
    end

    def [](store_id)
      @stores[store_id.to_sym]
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
      table ||= Masamune::Schema::Dimension.new(id: id, type: :mini) if options[:degenerate]
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
      format_options = options.extract!(:format, :headers)
      @context.push(options)
      yield if block_given?
      store = HasFormat.new(@context, format_options)
      @context.files[id] = HasMap.new Masamune::Schema::Table.new(@context.options.merge(id: id, type: :stage, store: store))
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
      @context.options[:function] = block.to_proc
      from.maps[to] ||= Masamune::Schema::Map.new(@context.options.merge(source: from, target: to))
    ensure
      @context.pop
    end

    def load(file)
      case file
      when /\.rb\Z/
        instance_eval(File.read(file), file)
      when /\.psql\Z/
        @stores[:postgres].extra << file
      when /\.hql\Z/
        @stores[:hive].extra << file
      end
    end

    private

    def dereference_column(id, options = {})
      store_id = id.split(/\./).reverse.last.try(:to_sym)
      context = store_id && @stores.key?(store_id) ? @stores[store_id] : @context
      context.dereference_column(id, options)
    end

    def fact_attributes(grain = [])
      return [{}] unless grain.any?
      grain.map { |x| { grain: x } }
    end
  end
end
