module Masamune::Transform
  module StageFact
    extend ActiveSupport::Concern

    def stage_fact(source, target, date)
      Operator.new(__method__, source: source, target: target, date: date, presenters: { postgres: Postgres })
    end

    private

    class Postgres < SimpleDelegator
      include Masamune::LastElement

      def insert_columns(source)
        shared_columns(source).values.map do |columns|
          column = columns.first
          if reference = column.reference
            reference.foreign_key_name
          else
            column.name
          end
        end
      end

      def insert_values(source)
        shared_columns(source).values.map do |columns|
          column = columns.first
          if reference = column.reference
            reference.surrogate_key.qualified_name
          else
            column.qualified_name
          end
        end
      end
      method_with_last_element :insert_values

      def join_conditions(source)
        join_columns = shared_columns(source).values.flatten.lazy
        join_columns = join_columns.select { |column| column.reference }.lazy
        join_columns = join_columns.group_by { |column| column.reference }.lazy

        dependencies = Masamune::TopologicalHash.new
        conditions = Hash.new { |h,k| h[k] = [] }
        join_columns.each do |reference, columns|
          columns.each do |column|
            dependencies[reference.name] = []
            cross_references = cross_references(column)
            coalesce_values = []

            if cross_references.any?
              dependencies[reference.name] += cross_references.map { |reference, _| reference.name }
              coalesce_values << cross_references.map { |_, column| column.qualified_name }
            end

            if column.adjacent.try(:default)
              coalesce_values << column.adjacent.sql_value(column.adjacent.try(:default))
            end

            conditions[reference.name] << (coalesce_values.any? ?
              "#{column.foreign_key_name} = COALESCE(#{column.qualified_name}, #{coalesce_values.join(', ')})" :
              "#{column.foreign_key_name} = #{column.qualified_name}")
          end
          if reference.type == :two || reference.type == :four
            join_key_a = "TO_TIMESTAMP(#{source.time_key.qualified_name}) BETWEEN #{reference.start_key.qualified_name} AND COALESCE(#{reference.end_key.qualified_name}, 'INFINITY')"
            join_key_b = "TO_TIMESTAMP(#{source.time_key.qualified_name}) < #{reference.start_key.qualified_name} AND #{reference.version_key.qualified_name} = 1"
            conditions[reference.name] << "((#{join_key_a}) OR (#{join_key_b}))"
          end
        end
        conditions.slice(*dependencies.tsort)
      end

      private

      def cross_references(column)
        return {} unless column.natural_key || column.adjacent.try(:natural_key)
        {}.tap do |result|
          references.each do |_, reference|
            if reference.id != column.reference.id && reference.columns[column.id]
              result[reference] = reference.columns[column.id]
            end
          end
        end
      end
    end
  end
end
