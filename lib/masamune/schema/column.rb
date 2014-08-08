require 'json'

module Masamune::Schema
  class Column
    attr_accessor :id
    attr_accessor :type
    attr_accessor :sub_type
    attr_accessor :null
    attr_accessor :strict
    attr_accessor :default
    attr_accessor :index
    attr_accessor :unique
    attr_accessor :primary_key
    attr_accessor :surrogate_key
    attr_accessor :degenerate_key
    attr_accessor :reference
    attr_accessor :parent
    attr_accessor :debug

    def initialize(id: id, type: :integer, sub_type: nil, null: false, strict: true, default: nil, index: false, unique: false, primary_key: false, surrogate_key: false, degenerate_key: false, reference: nil, parent: nil, debug: false)
      self.id         = id
      @type           = type
      @sub_type       = sub_type
      @null           = null
      @strict         = strict
      @default        = default
      @index          = index
      @unique         = unique
      @primary_key    = primary_key
      @surrogate_key  = surrogate_key
      @degenerate_key = degenerate_key
      @reference      = reference
      @parent         = parent
      @debug          = debug

      initialize_default_attributes!
    end

    def id=(id)
      @id = id.to_sym
    end

    def name
      if reference && reference.columns.include?(@id)
        "#{reference.name}_#{@id}".to_sym
      else
        @id
      end
    end

    def foreign_key_name
      "#{reference.name}.#{@id}".to_sym if reference
    end

    def compact_name
      if reference
        "#{reference.id}.#{@id}".to_sym
      else
        @id
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
        parent.type == :stage ? 'JSON' : 'HSTORE'
      when :json, :yaml
        'JSON'
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
      when :json, :yaml, :key_value
        value.to_json
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
        value.nil? ? nil : value.to_i
      when :yaml
        value.nil? ? {} : ruby_key_value(YAML.load(value))
      else
        value
      end
    end

    def sql_function?(value)
      value =~ /\(\)\Z/
    end

    def as_psql
      [name, sql_type(primary_key), *sql_constraints, sql_reference, sql_default].compact.join(' ')
    end

    class << self
      def dereference_column_name(name)
        if name =~ /\./
          reference_name, column_name = name.to_s.split('.')
          [reference_name.to_sym, column_name.to_sym]
        else
          [nil, name.to_sym]
        end
      end
    end

    private

    def sql_constraints
      [].tap do |constraints|
        constraints << 'NOT NULL' unless null || primary_key || !strict || parent.temporary?
        constraints << 'PRIMARY KEY' if primary_key
      end
    end

    def sql_default
      "DEFAULT #{sql_value(default)}" unless default.nil?
    end

    def sql_reference
      return if parent.temporary?
      if reference && reference.primary_key.type == type
        "REFERENCES #{reference.name}(#{reference.primary_key.name})"
      end
    end

    def initialize_default_attributes!
      self.default = 'uuid_generate_v4()' if primary_key && type == :uuid
    end

    def ruby_key_value(hash)
      case sub_type
      when :boolean
        Hash[hash.map { |key, value| ruby_boolean_key_value(key, value) }.compact]
      else
        hash
      end
    end

    def ruby_boolean_key_value(key, value)
      case value
      when true, '1', 1
        [key, true]
      when false, '0', 0
        [key, false]
      end
    end
  end
end
