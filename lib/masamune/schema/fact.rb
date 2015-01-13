module Masamune::Schema
  class Fact < Table
    attr_accessor :partition

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
      @stage_table = super.tap do |stage|
        stage.columns.each do |_, column|
          column.unique = false
        end
      end
    end

    def partition_table_name(date)
      partition_rule.bind_date(date).table
    end

    def partition_table_constraints(date)
      "CHECK (time_key >= #{partition_rule.bind_date(date).start_time.to_i} AND time_key < #{partition_rule.bind_date(date).stop_time.to_i})"
    end

    def partition_rule
      @partition_rule = Masamune::DataPlanRule.new(nil, :tmp, :target, table: name, partition: @partition)
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
  end
end
