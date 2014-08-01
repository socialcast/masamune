module Masamune::Schema
  class Row
    attr_accessor :reference
    attr_accessor :values
    attr_accessor :default
    attr_accessor :strict
    attr_accessor :debug

    def initialize(reference: nil, values: {}, default: false, name: nil, strict: true, debug: false)
      @values  = values.symbolize_keys
      @default = default
      @name    = name
      @strict  = strict
      @debug   = debug

      @name  ||= 'default' if default
      self.reference = reference
    end

    def reference=(reference)
      @reference = reference
      normalize_values! if @reference
    end

    def name
      return unless @name
      "#{@name}_#{reference.table_name}_#{reference.primary_key.name}()"
    end

    def surrogate_name
      return unless reference.surrogate_key && @name
      "#{@name}_#{reference.surrogate_key.name}()"
    end

    def insert_constraints
      values.map { |key, value| "#{key} = #{reference.columns[key].sql_value(value)}" }.compact
    end

    def insert_columns
      values.keys
    end

    def insert_values
      values.map { |key, value| reference.columns[key].sql_value(value) }
    end

    def to_hash
      values.with_indifferent_access
    end

    def to_csv
      {}.tap do |result|
        reference.columns.each do |_, column|
          result[column.name] = column.csv_value(values[column.name])
        end
      end.values.to_csv
    end

    private

    def normalize_values!
      result = {}
      reference.columns.each do |name, column|
        if @values.key?(column.compact_name)
          value = @values[column.compact_name]
          result[name] = column.ruby_value(value)
        end
      end

      if strict && @values.length > result.length
        raise ArgumentError, "#{@values} contains undefined columns"
      end

      @values = result
    end
  end
end
