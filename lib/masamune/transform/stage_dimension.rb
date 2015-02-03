module Masamune::Transform
  module StageDimension
    extend ActiveSupport::Concern

    def stage_dimension(source, target)
      Operator.new(__method__, source: source, target: target, presenters: { postgres: Postgres })
    end

    private

    class Postgres < Presenter
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
            reference.surrogate_key.qualified_name(reference.label)
          elsif column.type == :json || column.type == :yaml || column.type == :key_value
            "json_to_hstore(#{column.qualified_name})"
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
