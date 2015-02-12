module Masamune::Transform
  module RollupFact
    extend ActiveSupport::Concern

    def rollup_fact(source, target, date)
      raise ArgumentError, "#{source.name} must have date_column to rollup" unless source.date_column
      raise ArgumentError, "#{target.name} must have date_column to rollup" unless target.date_column
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
      end

      def insert_values(source)
        values = []
        values << "(#{first_date_surrogate_key})"
        source.columns.each do |_, column|
          next unless column.reference
          next if column.reference.type == :date
          values << column.qualified_name
        end
        source.measures.each do |_ ,measure|
          values << measure.aggregate_value
        end
        values << "(#{first_date_time_key})"
        values
      end
      method_with_last_element :insert_values

      def join_conditions(source)
        {
          source.date_column.reference.name => [
            "#{source.date_column.reference.surrogate_key.qualified_name} = #{source.date_column.qualified_name}"
          ]
        }
      end

      def group_by(source)
        group_by = []
        group_by << date_column.reference.columns[rollup_key].qualified_name
        source.columns.each do |_, column|
          next unless column.reference
          next if column.reference.type == :date
          group_by << column.qualified_name
        end
        group_by
      end
      method_with_last_element :group_by

      private

      def rollup_key
        case grain
        when :hourly
        when :daily
          :date_epoch
        when :monthly
          :month_epoch
        end
      end

      def date_key
        :date_id
      end

      def first_date_surrogate_key
        <<-EOS.gsub(/\s+/, ' ').strip
          SELECT
            #{date_column.reference.surrogate_key.name}
          FROM
            #{date_column.reference.name} d
          WHERE
            d.#{rollup_key} = #{date_column.reference.columns[rollup_key].qualified_name}
          ORDER BY
            d.#{date_key}
          LIMIT 1
        EOS
      end

      def first_date_time_key
        <<-EOS.gsub(/\s+/, ' ').strip
          SELECT
            #{rollup_key}
          FROM
            #{date_column.reference.name} d
          WHERE
            d.#{rollup_key} = #{date_column.reference.columns[rollup_key].qualified_name}
          ORDER BY
            d.#{date_key}
          LIMIT 1
        EOS
      end
    end
  end
end
