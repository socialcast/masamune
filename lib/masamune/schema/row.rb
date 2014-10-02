module Masamune::Schema
  class Row
    DEFAULT_ATTRIBUTES =
    {
      id:       nil,
      values:   {},
      default:  false,
      strict:   true,
      parent:   nil,
      debug:    false
    }

    DEFAULT_ATTRIBUTES.keys.each do |attr|
      attr_accessor attr
    end

    def initialize(opts = {})
      opts.symbolize_keys!
      DEFAULT_ATTRIBUTES.merge(opts).each do |name, value|
        send("#{name}=", value)
      end
      self.id ||= :default if default
    end

    def id=(id)
      @id = id.to_sym if id
    end

    def values=(values)
      @values = values.symbolize_keys
    end

    def parent=(parent)
      @parent = parent
      normalize_values! if @parent
    end

    def name(column = nil)
      return unless @id
      if column
        "#{@id}_#{column.name}()"
      else
        "#{@id}_#{parent.name}_#{parent.primary_key.name}()"
      end
    end

    def surrogate_keys
      parent.surrogate_keys.select do |column|
        values.keys.include?(column.name) && !column.sql_function?(values[column.name])
      end
    end

    def insert_constraints
      values.map { |key, value| "#{key} = #{parent.columns[key].sql_value(value)}" }.compact
    end

    def insert_columns
      values.keys
    end

    def insert_values
      values.map { |key, value| parent.columns[key].sql_value(value) }
    end

    def to_hash
      values.with_indifferent_access
    end

    def to_csv
      [].tap do |result|
        parent.columns.each do |_, column|
          result << column.csv_value(values[column.name])
        end
      end.to_csv
    end

    private

    def normalize_values!
      result = {}
      parent.columns.each do |_, column|
        if @values.key?(column.compact_name)
          value = @values[column.compact_name]
          result[column.name] = column.ruby_value(value)
        end
      end

      if strict && @values.length > result.length
        raise ArgumentError, "#{@values} contains undefined columns"
      end

      @values = result
    end
  end
end
