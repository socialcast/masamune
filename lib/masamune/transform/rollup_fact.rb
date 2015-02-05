module Masamune::Transform
  module RollupFact
    extend ActiveSupport::Concern

    def rollup_fact(source, target, date)
      Operator.new __method__, source: source.partition_table(date), target: target.partition_table(date), presenters: { postgres: Postgres }
    end

    private

    class Postgres < SimpleDelegator
      include Masamune::LastElement

      def insert_columns(source)
        source.columns.map do |_, column|
          next if column.id == :last_modified_at
          column.name
        end.compact
=begin
        shared_columns(source).values.map do |columns|
          columns.first.name
        end + source.measures.values.map(&:name)
=end
      end

      def insert_values(source)
        source.columns.map do |_, column|
          next if column.id == :last_modified_at
          column.aggregate_value
        end.compact
=begin
        shared_columns(source).values.map do |columns|
          columns.first.qualified_name
        end + source.measures.values.map(&:aggregate_value)
=end
      end
      method_with_last_element :insert_values

      def group_by(source)
        source.columns.map do |_, column|
          next unless column.reference
          column.qualified_name
        end.compact
      end
      method_with_last_element :group_by

      def join_conditions(source)
        [[source.columns[:date].qualified_name, source.columns[:date].qualified_name]]
      end
    end
  end
end
