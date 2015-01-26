module Masamune::Transform
  module SnapshotDimension
    extend ActiveSupport::Concern

    def snapshot_dimension(source, target, order = 'DESC')
      Operator.new(__method__, source: source, target: target, order: order, presenters: { psql: Postgres })
    end

    private

    class Postgres < Presenter
      def insert_columns(source = nil)
        consolidated_columns.map { |_, column| column.name }
      end

      def insert_view_values
        consolidated_columns.map { |_, column| column.name }
      end

      def insert_view_constraints
        consolidated_columns.reject { |_, column| column.null }.map { |_, column| "#{column.name} IS NOT NULL" }
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
            "hstore_merge(#{column.name}_now) OVER #{window} - hstore_merge(#{column.name}_was) OVER #{window} AS #{column.name}"
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
