module Masamune::Transform
  module BulkUpsert
    extend ActiveSupport::Concern

    def bulk_upsert(source, target)
      Operator.new(__method__, source: source, target: target, presenters: { postgres: Postgres})
    end

    private

    class Postgres < SimpleDelegator
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
