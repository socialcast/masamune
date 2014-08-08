module Masamune::Schema
  class Fact < Table
    def initialize(o)
      super
      initialize_fact_columns!
      foreign_key_columns.each do |column|
        column.index = true
      end
    end

    def name
      "#{id}_fact"
    end

    def type
      :fact
    end

    alias measures columns

    private

    def initialize_primary_key_column!
    end

    def initialize_fact_columns!
      initialize_column! id: 'last_modified_at', type: :timestamp, default: 'NOW()'
    end
  end
end
