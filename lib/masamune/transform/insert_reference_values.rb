require 'masamune/transform/bulk_upsert'

module Masamune::Transform
  module InsertReferenceValues
    include BulkUpsert

    extend ActiveSupport::Concern

    def insert_reference_values(source, target)
      operators = []
      target.insert_references.each do |_, reference|
        # FIXME add spec
        #target = Target.new(reference)
        #next unless target.insert_columns(source).any?
        operators << Operator.new(__method__, source: source, target: reference, presenters: { psql: Postgres })
        operators << bulk_upsert(reference.stage_table, reference)
      end
      Operator.new *operators
    end

    private

    class Postgres < Presenter
      def insert_columns(source)
        source.shared_columns(stage_table).map { |_, columns| columns.first.name }
      end

      def insert_values(source)
        source.shared_columns(stage_table).map do |column, _|
          if column.adjacent.try(:default)
            "COALESCE(#{column.name}, #{column.adjacent.sql_value(column.adjacent.default)})"
          else
            column.name
          end
        end
      end
      method_with_last_element :insert_values

      def insert_constraints(source)
        source.shared_columns(stage_table).reject { |column, _| column.null || column.default || column.adjacent.try(:default) }.map { |column, _| "#{column.name} IS NOT NULL"}
      end
      method_with_last_element :insert_constraints
    end
  end
end
