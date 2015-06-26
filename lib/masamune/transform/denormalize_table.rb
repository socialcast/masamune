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

module Masamune::Transform
  module DenormalizeTable
    extend ActiveSupport::Concern

    def denormalize_table(target, columns = [])
      Operator.new(__method__, target: target, columns: columns, presenters: { postgres: Postgres })
    end

    private

    class Postgres < SimpleDelegator
      include Masamune::LastElement

      def select_columns(column_names)
        column_names.map do |column_name|
          next unless column = dereference_column_name(column_name)
          if column.reference
            "#{column.foreign_key_name} AS #{column.name}"
          else
            column.qualified_name
          end
        end.compact
      end
      method_with_last_element :select_columns

      def join_conditions(column_names)
        {}.tap do |conditions|
          column_names.each do |column_name|
            next unless column = dereference_column_name(column_name)
            next unless column.reference
            adjacent_reference = references[column.reference.id]
            next unless adjacent_reference
            adjacent_column = columns[adjacent_reference.foreign_key_name]
            next unless adjacent_column
            conditions[column.reference.name] = "#{column.reference.surrogate_key.qualified_name} = #{adjacent_column.qualified_name}"
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
