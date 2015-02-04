module Masamune::Schema
  class Fact < Table
    SUPPORTED_GRAINS = %(hourly daily monthly)

    attr_accessor :grain
    attr_accessor :partition
    attr_accessor :range

    def initialize(opts = {})
      opts.symbolize_keys!
      self.grain = opts.delete(:grain)
      raise ArgumentError, "unknown grain '#{grain}'" unless SUPPORTED_GRAINS.include?(grain.to_s)
      @partition = opts.delete(:partition)
      super opts.reverse_merge(type: :fact)
      initialize_fact_columns!
      foreign_key_columns.each do |column|
        column.index << column.name
      end
      time_key.index << time_key.name
    end

    alias_method :measures, :columns

    def id
      [@id, grain].compact.join('_').to_sym
    end

    def suffix
      inherited = super
      [*inherited.split('_'), range.try(:suffix)].compact.uniq.join('_')
    end

    def time_key
      columns.values.detect { |column| column.id == :time_key }
    end

    def stage_table(*a)
      super.tap do |stage|
        stage.store = store
        stage.range = range
        stage.columns.each do |_, column|
          column.unique = false
        end
      end
    end

    def partition_table(date)
      partition_range = partition_rule.bind_date(date)
      @partition_tables ||= {}
      @partition_tables[partition_range] ||= self.class.new id: id, store: store, columns: partition_table_columns, parent: self, range: partition_range
    end

    def constraints
      return unless range
      "CHECK (time_key >= #{range.start_time.to_i} AND time_key < #{range.stop_time.to_i})"
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
