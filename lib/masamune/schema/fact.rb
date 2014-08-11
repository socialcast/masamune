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
