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
        public_send("#{name}=", value)
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
        "#{@id}_#{parent.name}_#{parent.surrogate_key.name}()"
      end
    end

    def natural_keys
      parent.natural_keys.select do |column|
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

    def headers
      values.keys
    end

    def serialize
      [].tap do |result|
        values.each do |key, value|
          result << @columns[key].csv_value(value)
        end
      end
    end

    private

    def normalize_values!
      result = {}
      @columns = {}
      values.each do |key, value|
        next unless key
        reference_name, column_name = Column::dereference_column_name(key)
        if reference_name && reference = parent.references[reference_name]
          if column = reference.columns[column_name]
            @columns[column.reference_name(reference.label)] = column
            result[column.reference_name(reference.label)] = column.ruby_value(value)
          end
        elsif column = parent.columns[column_name]
          @columns[column.name] = column
          result[column.name] = column.ruby_value(value)
        elsif strict
          raise ArgumentError, "#{@values} contains undefined columns #{key}"
        end
      end
      @values = result
    end
  end
end
