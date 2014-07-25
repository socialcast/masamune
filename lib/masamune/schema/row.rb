module Masamune::Schema
  class Row
    attr_accessor :dimension
    attr_accessor :values
    attr_accessor :default

    def initialize(values: [], default: false, name: nil)
      @values  = values
      @default = default
      @name    = name
      @name  ||= 'default' if default
    end

    def name
      return unless @name
      "#{@name}_#{dimension.table_name}_#{dimension.primary_key.name}()"
    end

    def surrogate_name
      return unless dimension.surrogate_key && @name
      "#{@name}_#{dimension.surrogate_key.name}()"
    end

    def insert_constraints
      values.map { |key, value| "#{key} = #{dimension.columns[key].sql_value(value)}" }.compact
    end

    def insert_columns
      values.keys
    end

    def insert_values
      values.map { |key, value| dimension.columns[key].sql_value(value) }
    end

    def dimension=(dimension)
      @dimension = dimension
      validate_values!
    end

    private

    def validate_values!
      values.each do |record|
        undefined_columns = insert_columns - dimension.columns.keys
        raise ArgumentError, "#{insert_columns} contains undefined columns #{undefined_columns}" if undefined_columns.any?
      end
    end
  end
end
