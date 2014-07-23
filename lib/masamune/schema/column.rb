module Masamune::Schema
  class Column
    attr_accessor :name
    attr_accessor :type
    attr_accessor :null
    attr_accessor :default
    attr_accessor :index
    attr_accessor :unique
    attr_accessor :primary_key
    attr_accessor :reference

    def initialize(name: name, type: :integer, null: false, default: nil, index: false, unique: false, primary_key: false, reference: nil)
      @name        = name.to_sym
      @type        = type
      @null        = null
      @default     = default
      @index       = index
      @unique      = unique
      @primary_key = primary_key
      @reference   = reference

      initialize_default_attributes!
    end

    def to_s
      [sql_name, sql_type, *sql_constraints, sql_default, sql_reference].compact.join(' ')
    end

    private

    def sql_name
      name
    end

    def sql_type
      case type
      when :integer
        primary_key ? 'SERIAL' : 'INTEGER'
      when :string
        'VARCHAR'
      when :uuid
        'UUID'
      when :timestamp
        'TIMESTAMP'
      end
    end

    def sql_constraints
      [].tap do |constraints|
        constraints << 'NOT NULL' unless null || primary_key || default
        constraints << 'PRIMARY KEY' if primary_key
      end
    end

    def sql_default
      "DEFAULT #{default}" if default
    end

    def sql_reference
      "REFERENCES #{reference.table_name}(#{reference.primary_key.name})" if reference
    end

    def initialize_default_attributes!
      self.default = 'uuid_generate_v4()' if primary_key && type == :uuid
    end
  end
end
