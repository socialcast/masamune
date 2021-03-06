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

module Masamune::Transform::Postgres
  class SnapshotDimension
    def initialize(options = {})
      @target   = options[:target]
      @source   = options[:source]
      @order    = options[:order]
    end

    def locals
      { target: target, source: @source, order: @order }
    end

    def target
      TargetPresenter.new(@target)
    end

    class TargetPresenter < SimpleDelegator
      include Masamune::LastElement

      def insert_columns(_source = nil)
        consolidated_columns.map { |_, column| column.name }
      end

      def insert_view_values
        consolidated_columns.map { |_, column| column.name }
      end

      def insert_view_constraints
        consolidated_columns.reject { |_, column| !column.default.nil? || column.null }.map { |_, column| "#{column.name} IS NOT NULL" }
      end
      method_with_last_element :insert_view_constraints

      def window(*extra)
        (columns.values.select { |column| extra.delete(column.name) || column.natural_key || column.auto_reference }.map(&:name) + extra).uniq
      end

      def insert_values(opts = {})
        window = opts[:window]
        consolidated_columns.map do |_, column|
          if column.natural_key
            "#{column.name} AS #{column.name}"
          elsif column.type == :key_value
            "hstore_merge(#{column.name}) OVER #{window} AS #{column.name}"
          else
            "coalesce_merge(#{column.name}) OVER #{window} AS #{column.name}"
          end
        end
      end
      method_with_last_element :insert_values

      private

      def consolidated_columns
        unreserved_columns.reject { |_, column| column.surrogate_key }
      end
    end
  end
end
