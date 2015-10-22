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

require 'masamune/last_element'

module Masamune::Transform::Common
  class DenormalizeTable
    def initialize(options = {})
      @target   = options[:target]
      @columns  = options[:columns]
      @order_by = options[:order_by]
    end

    def locals
      { target: target, columns: @columns, order_by: @order_by }
    end

    def target
      TargetPresenter.new(@target)
    end

    private

    class TargetPresenter < SimpleDelegator
      include Masamune::LastElement

      def select_columns(column_names)
        column_names.map do |column_name|
          next unless column = dereference_column_name(column_name)
          if column.reference
            if column.reference.implicit || column.reference.degenerate
              "#{column.name} AS #{column.name}"
            else
              "#{column.foreign_key_name} AS #{column.name}"
            end
          else
            column.qualified_name
          end
        end.compact
      end
      method_with_last_element :select_columns

      def join_alias(reference)
        reference.label ? "#{reference.name} AS #{[reference.label, reference.name].compact.join('_')}" : reference.name
      end

      def join_conditions(column_names)
        {}.tap do |conditions|
          column_names.each do |column_name|
            next unless column = dereference_column_name(column_name)
            next unless column.reference
            next if column.reference.degenerate
            adjacent_reference = references[column.reference.id]
            next unless adjacent_reference
            adjacent_column = columns[adjacent_reference.foreign_key_name]
            next unless adjacent_column
            conditions[join_alias(column.reference)] = "#{column.reference.surrogate_key.qualified_name(column.reference.label)} = #{adjacent_column.qualified_name}"
          end
        end
      end

      def order_by_columns(column_names)
        column_names.map do |column_name|
          next unless column = dereference_column_name(column_name)
          column.name
        end.compact
      end
      method_with_last_element :order_by_columns
    end
  end
end
