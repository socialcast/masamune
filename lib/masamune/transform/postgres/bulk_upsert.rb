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

module Masamune::Transform::Postgres
  class BulkUpsert
    def initialize(options = {})
      @target   = options[:target]
      @source   = options[:source]
    end

    def locals
      { target: target, source: @source }
    end

    def target
      TargetPresenter.new(@target)
    end

    private

    class TargetPresenter < SimpleDelegator
      include Masamune::LastElement

      def update_columns
        columns.values.reject { |column| reserved_column_ids.include?(column.id) || column.surrogate_key || column.natural_key || column.unique.any? || column.auto_reference || column.ignore }
      end
      method_with_last_element :update_columns

      def insert_columns
        columns.values.reject { |column| column.surrogate_key || column.auto_reference || column.ignore }
      end
      method_with_last_element :insert_columns

      def unique_columns
        columns.values.select { |column| column.unique.any? && !column.null }
      end
      method_with_last_element :unique_columns
    end
  end
end

