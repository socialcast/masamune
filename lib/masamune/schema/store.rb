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

module Masamune::Schema
  class Store
    include Masamune::HasEnvironment

    SUPPORTED_ATTRIBUTES = %(table dimension fact file event)

    DEFAULT_ATTRIBUTES =
    {
      type:            nil,
      format:          ->(store) { store.type == :postgres ? :csv : :tsv },
      json_encoding:   ->(store) { store.type == :postgres ? :quoted : :raw },
      headers:         ->(store) { store.type == :postgres ? true : false },
      debug:           false
    }

    DEFAULT_ATTRIBUTES.keys.each do |attr|
      attr_accessor attr
    end

    attr_accessor :tables
    attr_accessor :dimensions
    attr_accessor :facts
    attr_accessor :files
    attr_accessor :events
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
      @events     = {}.with_indifferent_access
      @references = {}.with_indifferent_access
      @extra      = []
    end

    def method_missing(method, *args, &block)
      if type == :files
        files[method]
      else
        *attribute_name, attribute_type = method.to_s.split('_')
        raise ArgumentError, "unknown attribute type '#{attribute_type}'" unless SUPPORTED_ATTRIBUTES.include?(attribute_type)
        self.send(attribute_type.pluralize)[attribute_name.join('_')]
      end
    end

    def dereference_column(id, options = {})
      column_id, reference_id = id.to_s.split(/\./).reverse
      column_options = options.dup
      column_options.merge!(id: column_id)

      if reference = references[reference_id]
        column_options.merge!(reference: reference)
      else
        raise ArgumentError, "dimension #{reference_id} not defined"
      end if reference_id

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
        else
          result << file if order == :pre
        end
      end
      result.to_a
    end
  end
end
