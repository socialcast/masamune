module Masamune::Transform
  module DeduplicateDimension
    extend ActiveSupport::Concern

    def deduplicate_dimension(source, target)
      Operator.new(__method__, source: source, target: target, presenters: { postgres: Postgres })
    end

    private

    class Postgres < Presenter
      def insert_columns(source = nil)
        consolidated_columns.map { |_, column| column.name }
      end

      def insert_view_values
        consolidated_columns.map { |_, column| column.name }
      end

      def window(*extra)
        (columns.values.select { |column| extra.delete(column.name) || column.natural_key || column.auto_reference }.map(&:name) + extra).uniq
      end

      private

      def consolidated_columns
        unreserved_columns.reject { |_, column| column.surrogate_key }
      end
    end
  end
end
