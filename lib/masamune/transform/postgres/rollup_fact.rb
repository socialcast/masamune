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

module Masamune::Transform::Postgres
  class RollupFact
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

    private

    class TargetPresenter < SimpleDelegator
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
        values << calculated_date_key(source)
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
        values << calculated_time_key(source)
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
        group_by << calculated_date_key(source)
        shared_columns(source).values.map do |columns|
          column = columns.first
          next unless column.reference
          next if column.reference.type == :date
          next if column.auto_reference
          group_by << column.qualified_name
        end
        group_by << calculated_time_key(source)
        group_by
      end
      method_with_last_element :group_by

      private

      def calculated_date_key(source)
        case grain
        when :hourly, :daily
          "#{source.date_column.qualified_name}"
        when :monthly
          "to_char(date_trunc('month',#{source.date_column.qualified_name}::text::date),'YYYYMMDD')::integer"
        end
      end

      def calculated_time_key(source)
        case grain
        when :hourly
          "(#{source.time_key.qualified_name} - (#{source.time_key.qualified_name} % #{1.hour.seconds}))"
        when :daily
          "extract(EPOCH from #{source.date_column.qualified_name}::text::date)"
        when :monthly
          "extract(EPOCH from date_trunc('month',#{source.date_column.qualified_name}::text::date))"
        end
      end
    end
  end
end
