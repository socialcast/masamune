#  The MIT License (MIT)
#
#  Copyright (c) 2014-2015, VMware, Inc. All Rights Reserved.
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
        values = []
        shared_columns(source).values.map do |columns|
          column = columns.first
          next if column.id == :last_modified_at
          next if column.auto_reference
          values << column.name
        end
        measures.each do |_ ,measure|
          values << measure.name
        end
        values << time_key.name
        values.compact
      end

      def insert_values(source)
        values = []
        values << "(#{first_date_surrogate_key})"
        shared_columns(source).values.map do |columns|
          column = columns.first
          next unless column.reference
          next if column.reference.type == :date
          next if column.auto_reference
          values << column.qualified_name
        end
        source.measures.each do |_ ,measure|
          values << measure.aggregate_value
        end
        values << "(#{floor_time_key(source)})"
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
        shared_columns(source).values.map do |columns|
          column = columns.first
          next unless column.reference
          next if column.reference.type == :date
          next if column.auto_reference
          group_by << column.qualified_name
        end
        group_by << "(#{floor_time_key(source)})" if grain == :hourly
        group_by
      end
      method_with_last_element :group_by

      private

      def rollup_key
        case grain
        when :hourly
          :date_epoch
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

      def floor_time_key(source)
        case grain
        when :hourly
          "#{source.time_key.qualified_name} - (#{source.time_key.qualified_name} % #{1.hour.seconds})"
        when :daily, :monthly
          first_date_time_key
        end
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
