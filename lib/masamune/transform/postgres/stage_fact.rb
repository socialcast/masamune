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
  class StageFact
    def initialize(options = {})
      @target = options[:target]
      @source = options[:source]
      @date   = options[:date]
    end

    def locals
      { target: target, source: @source, date: @date }
    end

    def target
      TargetPresenter.new(@target)
    end

    class TargetPresenter < SimpleDelegator
      include Masamune::LastElement

      def insert_columns(source)
        shared_columns(source).values.map do |columns|
          column = columns.first
          if column.reference
            column.reference.foreign_key_name
          else
            column.name
          end
        end
      end

      def insert_values(source)
        shared_columns(source).values.map do |columns|
          column = columns.first
          if !column.degenerate? && column.reference
            value = column.reference.surrogate_key.qualified_name(column.reference.label)
            column.reference.unknown ? "COALESCE(#{value}, #{column.reference.unknown})" : value
          else
            column.qualified_name
          end
        end
      end
      method_with_last_element :insert_values

      def join_alias(reference)
        reference.label ? "#{reference.name} AS #{[reference.label, reference.name].compact.join('_')}" : reference.name
      end

      def join_conditions(source)
        join_columns = shared_columns(source).values.flatten
        join_columns = join_columns.select(&:reference)
        join_columns = join_columns.group_by(&:reference)

        dependencies = Masamune::TopologicalHash.new
        conditions = Hash.new { |h, k| h[k] = OpenStruct.new(type: 'INNER', conditions: []) }
        join_columns.each do |reference, columns|
          reference_name = join_alias(reference)
          columns.each do |column|
            next if column.degenerate?
            dependencies[reference_name] ||= []
            cross_references = cross_references(column)

            coalesce_values = []

            if cross_references.any?
              dependencies[reference_name] += cross_references.map { |cross_reference, _| join_alias(cross_reference) }
              coalesce_values << cross_references.map { |cross_reference, cross_column| cross_column.qualified_name(cross_reference.label) }
            end

            if column.reference
              column.reference.auto_surrogate_keys.each do |auto_surrogate_key|
                next unless auto_surrogate_key.default
                conditions[reference_name].conditions << "#{auto_surrogate_key.qualified_name(reference.label)} = #{auto_surrogate_key.default}"
              end
            end

            if column.reference && !column.reference.default.nil? && column.adjacent.natural_key
              coalesce_values << column.reference.default(column.adjacent)
            elsif column.adjacent && !column.adjacent.default.nil?
              coalesce_values << column.adjacent.sql_value(column.adjacent.default)
            end

            conditions[reference_name].conditions <<
            if coalesce_values.any?
              "#{column.foreign_key_name} = COALESCE(#{column.qualified_name}, #{coalesce_values.join(', ')})"
            else
              "#{column.foreign_key_name} = #{column.qualified_name}"
            end
          end

          if reference.type == :two || reference.type == :four
            join_key_a = "TO_TIMESTAMP(#{source.time_key.qualified_name}) BETWEEN #{reference.start_key.qualified_name(reference.label)} AND COALESCE(#{reference.end_key.qualified_name(reference.label)}, 'INFINITY')"
            join_key_b = "TO_TIMESTAMP(#{source.time_key.qualified_name}) < #{reference.start_key.qualified_name(reference.label)} AND #{reference.version_key.qualified_name(reference.label)} = 1"
            conditions[reference_name].conditions << "((#{join_key_a}) OR (#{join_key_b}))"
          end

          conditions[reference_name].type = 'LEFT' if reference.unknown
          conditions[reference_name].conditions.uniq!
        end
        conditions.slice(*dependencies.tsort)
      end

      private

      def cross_references(column)
        return {} unless column.natural_key || column.adjacent.try(:natural_key)
        {}.tap do |result|
          column.reference.through.each do |reference_id|
            reference = references[reference_id]
            if reference.columns[column.id]
              result[reference] = reference.columns[column.id]
            end
          end
        end
      end
    end
  end
end
