module Masamune::Schema
  class Column
    attr_accessor :name
    attr_accessor :type
    attr_accessor :null
    attr_accessor :default
    attr_accessor :index
    attr_accessor :unique
    attr_accessor :primary_key
    attr_accessor :surrogate_key
    attr_accessor :reference
    attr_accessor :transform

    def initialize(name: name, type: :integer, null: false, default: nil, index: false, unique: false, primary_key: false, surrogate_key: false, reference: nil, transform: nil)
      @name          = name.to_sym
      @type          = type
      @null          = null
      @default       = default
      @index         = index
      @unique        = unique
      @primary_key   = primary_key
      @surrogate_key = surrogate_key
      @reference     = reference
      @transform     = transform

      initialize_default_attributes!
    end

    def to_s
      [sql_name, sql_type(primary_key), *sql_constraints, sql_reference, sql_default].compact.join(' ')
    end

    def sql_name
      name
    end

    def sql_type(for_primary_key = false)
      case type
      when :integer
        for_primary_key ? 'SERIAL' : 'INTEGER'
      when :string
        'VARCHAR'
      when :uuid
        'UUID'
      when :timestamp
        'TIMESTAMP'
      when :boolean
        'BOOLEAN'
      when :key_value
        'HSTORE'
      end
    end

    def sql_value(value)
      return value if sql_function?(value)
      case type
      when :boolean
        value ? 'TRUE' : 'FALSE'
      when :string
        "'#{value}'"
      else
        value
      end
    end

    def sql_function?(value)
      value =~ /\(\)\Z/
    end

    private

    def sql_constraints
      [].tap do |constraints|
        constraints << 'NOT NULL' unless null || primary_key || !default.nil?
        constraints << 'PRIMARY KEY' if primary_key
      end
    end

    def sql_default
      "DEFAULT #{sql_value(default)}" unless default.nil?
    end

    def sql_reference
      "REFERENCES #{reference.table_name}(#{reference.primary_key.name})" if reference
    end

    def initialize_default_attributes!
      self.default = 'uuid_generate_v4()' if primary_key && type == :uuid
    end
  end
end
