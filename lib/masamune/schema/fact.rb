module Masamune::Schema
  class Fact < Table
    attr_accessor :partition

    def initialize(o = {})
      @partition = o.delete(:partition)
      super o.reverse_merge(type: :fact)
      initialize_fact_columns!
      foreign_key_columns.each do |column|
        column.index ||= true
      end
      time_key.index ||= true
    end

    alias measures columns

    def time_key
      columns.values.detect { |column| column.id == :time_key }
    end

    def insert_columns(source)
      shared_columns(source).values.map do |columns|
        column = columns.first
        if reference = column.reference
          reference.foreign_key_name
        else
          column.name
        end
      end
    end

    def insert_values(source)
      shared_columns(source).values.map do |columns|
        column = columns.first
        if reference = column.reference
          reference.primary_key.qualified_name
        else
          column.qualified_name
        end
      end
    end
    method_with_last_element :insert_values

    def join_conditions(source)
      join_columns = shared_columns(source).values.flatten.lazy
      join_columns = join_columns.select { |column| column.reference }.lazy
      join_columns = join_columns.group_by { |column| column.reference }.lazy
      join_columns.map do |reference, columns|
        conditions = columns.map do |column|
          if column.adjacent.try(:default)
            "#{column.foreign_key_name} = COALESCE(#{column.qualified_name}, #{column.adjacent.sql_value(column.adjacent.try(:default))})"
          else
            "#{column.foreign_key_name} = #{column.qualified_name}"
          end
        end
        if reference.type == :two || reference.type == :four
          conditions << "TO_TIMESTAMP(#{source.time_key.qualified_name}) BETWEEN #{reference.start_key.qualified_name} AND COALESCE(#{reference.end_key.qualified_name}, 'INFINITY')"
        end
        [reference.name, conditions]
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

    private

    def initialize_primary_key_column!
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
