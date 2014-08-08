module Masamune::Schema
  class Fact < Table
    def initialize(o)
      super
      initialize_fact_columns!
      foreign_key_columns.each do |column|
        column.index = true
      end
    end

    alias measures columns

    def type
      :fact
    end

    def table_name
      "#{name}_fact"
    end

    private

    def initialize_primary_key_column!
    end

    def initialize_fact_columns!
      initialize_column! name: 'last_modified_at', type: :timestamp, default: 'NOW()'
    end
  end
end
