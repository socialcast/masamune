#  The MIT License (MIT)
#
#  Copyright (c) 2014-2016, VMware, Inc. All Rights Reserved.
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

require 'masamune/has_environment'

module Masamune::Schema
  class Store
    include Masamune::HasEnvironment

    SUPPORTED_ATTRIBUTES = %(table dimension fact file).freeze

    DEFAULT_ATTRIBUTES =
    {
      type:            nil,
      format:          ->(store) { default_format(store) },
      json_encoding:   ->(store) { default_json_encoding(store) },
      headers:         ->(store) { default_headers(store) },
      debug:           false
    }.freeze

    DEFAULT_ATTRIBUTES.keys.each do |attr|
      attr_accessor attr
    end

    attr_accessor :tables
    attr_accessor :dimensions
    attr_accessor :facts
    attr_accessor :files
    attr_accessor :references

    class << self
      def types
        [:postgres, :hive, :files]
      end
    end

    def initialize(environment, opts = {})
      self.environment = environment
      opts.symbolize_keys!
      raise ArgumentError, 'required parameter type: missing' unless opts.key?(:type)
      raise ArgumentError, "unknown type: '#{opts[:type]}'" unless self.class.types.include?(opts[:type])
      DEFAULT_ATTRIBUTES.merge(opts).each do |name, value|
        public_send("#{name}=", value.respond_to?(:call) ? value.call(self) : value)
      end

      @tables     = {}.with_indifferent_access
      @dimensions = {}.with_indifferent_access
      @facts      = {}.with_indifferent_access
      @files      = {}.with_indifferent_access
      @references = {}.with_indifferent_access
      @extra      = []
    end

    def method_missing(method_name, *_args)
      *attribute_name, attribute_type = method_name.to_s.split('_')
      if type == :files
        files[method_name]
      elsif SUPPORTED_ATTRIBUTES.include?(attribute_type)
        send(attribute_type.pluralize)[attribute_name.join('_')]
      else
        super
      end
    end

    def respond_to_missing?(method_name, _include_private = false)
      *attribute_name, attribute_type = method_name.to_s.split('_')
      if type == :files
        files.key?(method_name)
      elsif SUPPORTED_ATTRIBUTES.include?(attribute_type)
        send(attribute_type.pluralize).key?(attribute_name.join('_'))
      else
        super
      end
    end

    def dereference_column(id, options = {})
      column_id, reference_id = id.to_s.split(/\./).reverse
      column_options = options.dup
      column_options[:id] = column_id

      if reference_id
        raise ArgumentError, "dimension #{reference_id} not defined" unless references[reference_id]
        column_options[:reference] = references[reference_id]
      end

      Masamune::Schema::Column.new(column_options)
    end

    def extra(order = nil)
      return @extra unless order
      result = Set.new
      @extra.each do |file|
        filename = File.basename(file)
        if filename =~ /\A\d+_/
          number = filename.split('_').first.to_i
          result << file if number <= 0 && order == :pre
          result << file if number > 0 && order == :post
        elsif order == :pre
          result << file
        end
      end
      result.to_a
    end

    class << self
      private

      def default_format(store)
        case store.type
        when :postgres then :csv
        when :hive then :tsv
        else :raw
        end
      end

      def default_headers(store)
        return false if store.format == :raw
        case store.type
        when :postgres then true
        else false
        end
      end

      def default_json_encoding(store)
        return :raw if store.format == :raw
        case store.type
        when :postgres then :quoted
        else :raw
        end
      end
    end
  end
end
