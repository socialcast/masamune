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

module Masamune::Schema
  class Fact < Table
    SUPPORTED_GRAINS = [:transaction, :hourly, :daily, :monthly]

    attr_accessor :grain
    attr_accessor :partition
    attr_accessor :range

    def initialize(opts = {})
      opts.symbolize_keys!
      self.grain = opts.delete(:grain)
      @partition = opts.delete(:partition)
      super opts.reverse_merge(type: :fact)
      initialize_fact_columns!
      foreign_key_columns.each do |column|
        column.index << column.name
      end
      time_key.index << time_key.name
    end

    def id
      [@id, grain].compact.join('_').to_sym
    end

    def grain=(grain = nil)
      return unless grain
      raise ArgumentError, "unknown grain '#{grain}'" unless SUPPORTED_GRAINS.include?(grain.to_sym)
      @grain = grain.to_sym
    end

    def suffix
      inherited = super
      [*inherited.split('_'), range.try(:suffix)].compact.uniq.join('_')
    end

    def date_column
      columns.select { |_, column| column && column.reference && column.reference.type == :date }.values.first
    end

    def time_key
      columns.values.detect { |column| column.id == :time_key }
    end

    def stage_table(options = {})
      super(options).tap do |stage|
        stage.id      = @id
        stage.suffix  = options[:suffix]
        stage.store   = store
        stage.range   = range
        stage.grain   = grain
        stage.columns.each do |_, column|
          column.unique = false
        end
      end
    end

    def partition_table(date)
      partition_range = partition_rule.bind_date(date)
      @partition_tables ||= {}
      @partition_tables[partition_range] ||= self.class.new(id: @id, store: store, columns: partition_table_columns, parent: self, range: partition_range, grain: grain, inherit: true)
    end

    def measures
      columns.select { |_, column| column.measure }
    end

    def constraints
      return unless range
      "CHECK (time_key >= #{range.start_time.to_i} AND time_key < #{range.stop_time.to_i})"
    end

    def reserved_column_ids
      case type
      when :fact
        [:time_key, :last_modified_at]
      else
        super
      end
    end

    private

    def initialize_surrogate_key_column!
    end

    def initialize_fact_columns!
      case type
      when :fact
        initialize_column! id: 'time_key', type: :integer, index: true
        initialize_column! id: 'last_modified_at', type: :timestamp, default: 'NOW()' unless store.type == :hive
      when :stage
        if inherit
          parent.reserved_columns.each do |_, column|
            initialize_column! column.as_hash
          end
        end
      end
    end

    def partition_rule
      @partition_rule ||= Masamune::DataPlan::Rule.new(nil, :tmp, :target, table: name, partition: @partition)
    end

    def partition_table_columns
      unreserved_columns.map { |_, column| column.dup }
    end
  end
end
