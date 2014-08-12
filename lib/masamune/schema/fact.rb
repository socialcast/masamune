module Masamune::Schema
  class Fact < Table
    def initialize(o)
      super o.reverse_merge(type: :fact)
      initialize_fact_columns!
      foreign_key_columns.each do |column|
        column.index ||= true
      end
    end

    alias measures columns

    def insert_columns(source)
      left_shared_columns(source).map do |column|
        if reference = column.reference
          reference.foreign_key_name
        else
          column.name
        end
      end
    end

    def insert_values(source)
      left_shared_columns(source).map do |column|
        if reference = column.reference
          reference.primary_key.qualified_name
        else
          column.qualified_name
        end
      end
    end
    method_with_last_element :insert_values

    def join_conditions(source)
      join_columns = right_shared_columns(source).lazy
      join_columns = join_columns.select { |column| column.reference }.lazy
      join_columns = join_columns.group_by { |column| column.reference }.lazy
      join_columns.map do |reference, columns|
        conditions = columns.map do |column|
          "#{column.foreign_key_name} = #{column.qualified_name}"
        end
        if reference.type == :two
          conditions << "TO_TIMESTAMP(#{source.name}.time_unix)::DATE BETWEEN #{reference.name}.start_at::DATE AND COALESCE(#{reference.name}.end_at::DATE, 'INFINITY')"
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

    private

    def initialize_primary_key_column!
    end

    def initialize_fact_columns!
      case type
      when :fact
        initialize_column! id: 'last_modified_at', type: :timestamp, default: 'NOW()'
      end
    end
  end
end
