module Masamune::Schema
  class Column
    attr_accessor :type
    attr_accessor :null
    attr_accessor :default
    attr_accessor :index
    attr_accessor :unique
    attr_accessor :primary_key
    attr_accessor :surrogate_key
    attr_accessor :reference
    attr_accessor :debug

    def initialize(name: name, type: :integer, null: false, default: nil, index: false, unique: false, primary_key: false, surrogate_key: false, reference: nil, debug: false)
      @name          = name.to_sym
      @type          = type
      @null          = null
      @default       = default
      @index         = index
      @unique        = unique
      @primary_key   = primary_key
      @surrogate_key = surrogate_key
      @reference     = reference
      @debug         = debug

      initialize_default_attributes!
    end

    def to_s
      [name, sql_type(primary_key), *sql_constraints, sql_reference, sql_default].compact.join(' ')
    end

    def name=(name)
      @name = name.to_sym
    end

    def name
      if reference && reference.columns.include?(@name)
        "#{reference.table_name}_#{@name}".to_sym
      else
        @name
      end
    end

    def foreign_key_name
      "#{reference.table_name}.#{@name}".to_sym if reference
    end

    def compact_name
      if reference
        "#{reference.name}.#{@name}".to_sym
      else
        @name
      end
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

    def csv_value(value)
      return value if sql_function?(value)
      case type
      when :boolean
        value ? 'TRUE' : 'FALSE'
      else
        value
      end
    end

    def ruby_value(value)
      return value if sql_function?(value)
      case type
      when :boolean
        case value
        when false, 0, '0', "'0'", 'FALSE'
          false
        when true, 1, '1', "'1'", 'TRUE'
          true
        end
      when :integer
        value.to_i
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
      if reference && reference.primary_key.type == type
        "REFERENCES #{reference.table_name}(#{reference.primary_key.name})"
      end
    end

    def initialize_default_attributes!
      self.default = 'uuid_generate_v4()' if primary_key && type == :uuid
    end
  end
end
