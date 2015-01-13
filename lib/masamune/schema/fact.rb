module Masamune::Schema
  class Fact < Table
    attr_accessor :partition
    attr_accessor :range

    def initialize(opts = {})
      opts.symbolize_keys!
      @partition = opts.delete(:partition)
      super opts.reverse_merge(type: :fact)
      initialize_fact_columns!
      foreign_key_columns.each do |column|
        column.index << column.name
      end
      time_key.index << time_key.name
    end

    alias_method :measures, :columns

    def time_key
      columns.values.detect { |column| column.id == :time_key }
    end

    #special handling for visitation table. Include hourly_snapshot in the name
    def name
      "#{id}_#{suffix}"
      s = "#{id}"
      if s.eql? "visitation"
        s = "#{id}_hourly_snapshot_#{suffix}"
      end
    end

    #special handling for visitation table. Create two rollup tables
    def as_psql
      s = "#{id}"
      super
      if s.eql? "visitation"
        output = []
        output << super
        output << Masamune::Template.render_to_string(rollup_template)
        output.join("\n")
      end
    end

    def stage_table(*a)
      super.tap do |stage|
        stage.range = range
        stage.columns.each do |_, column|
          column.unique = false
        end
      end
    end

    def partition_table(date)
      partition_range = partition_rule.bind_date(date)
      @partition_tables ||= {}
      @partition_tables[partition_range] ||= self.class.new id: id, columns: partition_table_columns, parent: self, range: partition_range, suffix: partition_range.suffix
    end

    def constraints
      return unless range
      "CHECK (time_key >= #{range.start_time.to_i} AND time_key < #{range.stop_time.to_i})"
    end

    def rollup_template
      ::File.expand_path(::File.join(__FILE__, '..', 'rollup.psql.erb'))
    end
    private

    def initialize_surrogate_key_column!
    end

    def initialize_fact_columns!
      case type
      when :fact
        initialize_column! id: 'time_key', type: :integer, index: true
        initialize_column! id: 'last_modified_at', type: :timestamp, default: 'NOW()'
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
