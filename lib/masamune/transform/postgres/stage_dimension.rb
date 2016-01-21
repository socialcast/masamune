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
  class StageDimension
    def initialize(options = {})
      @target = options[:target]
      @source = options[:source]
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

      def insert_columns(source)
        shared_columns(source).values.map do |columns|
          column = columns.first
          if reference = column.reference
            reference.foreign_key_name
          else
            column.name
          end
        end.compact
      end

      def insert_values(source)
        shared_columns(source).values.map do |columns|
          column = columns.first
          if reference = column.reference
            reference.surrogate_key.qualified_name(reference.label)
          elsif column.type == :json || column.type == :yaml || column.type == :key_value
            "json_to_hstore(#{column.qualified_name})"
          else
            column.qualified_name
          end
        end.compact
      end
      method_with_last_element :insert_values

      def join_conditions(source)
        join_columns = shared_columns(source).values.flatten
        join_columns = join_columns.select { |column| column.reference }
        join_columns = join_columns.group_by { |column| column.reference }

        conditions = Hash.new { |h,k| h[k] = Set.new }
        join_columns.each do |reference, columns|
          left_uniq = Set.new
          (columns + lateral_references(source, reference)).each do |column|
            left = reference.columns[column.id]
            next unless left_uniq.add?(left.qualified_name(reference.label))
            conditions[[reference.name, reference.alias]] << "#{left.qualified_name(reference.label)} = #{column.qualified_name}"
          end
        end
        conditions
      end

      def lateral_references(source, reference)
        source.shared_columns(reference).keys.reject { |column| column.auto_reference }
      end
    end
  end
end
