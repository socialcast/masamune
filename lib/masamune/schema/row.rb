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

module Masamune::Schema
  class Row
    DEFAULT_ATTRIBUTES =
    {
      id:       nil,
      values:   {},
      default:  false,
      strict:   true,
      parent:   nil,
      debug:    false
    }

    DEFAULT_ATTRIBUTES.keys.each do |attr|
      attr_accessor attr
    end

    def initialize(opts = {})
      opts.symbolize_keys!
      DEFAULT_ATTRIBUTES.merge(opts).each do |name, value|
        public_send("#{name}=", value)
      end
      self.id ||= :default if default
    end

    def id=(id)
      @id = id.to_sym if id
    end

    def values=(values)
      @values = values.symbolize_keys
    end

    def parent=(parent)
      @parent = parent
      normalize_values! if @parent
    end

    def name(column = nil)
      return unless @id
      if column
        "#{@id}_#{column.name}()"
      else
        "#{@id}_#{parent.name}_#{parent.surrogate_key.name}()"
      end
    end

    def natural_keys
      parent.natural_keys.select do |column|
        values.keys.include?(column.name) && !column.sql_function?(values[column.name])
      end
    end

    def insert_constraints
      values.map { |key, value| "#{key} = #{parent.columns[key].sql_value(value)}" }.compact
    end

    def insert_columns
      values.keys
    end

    def insert_values
      values.map { |key, value| parent.columns[key].sql_value(value) }
    end

    def to_hash
      values.with_indifferent_access
    end

    def headers
      @columns.map { |_, column| column.name }
    end

    def serialize
      [].tap do |result|
        values.each do |key, value|
          result << @columns[key].csv_value(value)
        end
      end
    end

    def sql_value(column)
      column.sql_value(values[column.name])
    end

    def missing_required_columns
      Set.new.tap do |missing|
        values.select do |key, value|
          column = @columns[key]
          missing << column if column.required_value? && value.nil?
        end
      end
    end

    private

    def normalize_values!
      result = {}
      @columns = {}
      values.each do |key, value|
        next unless key
        if column = parent.dereference_column_name(key)
          @columns[column.compact_name] = column
          result[column.compact_name] = column.ruby_value(value)
        elsif strict
          raise ArgumentError, "#{@values} contains undefined columns #{key}"
        end
      end
      @values = result
    end
  end
end
