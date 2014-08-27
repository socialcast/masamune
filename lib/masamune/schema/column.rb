require 'json'

module Masamune::Schema
  class Column
    attr_accessor :id
    attr_accessor :type
    attr_accessor :sub_type
    attr_accessor :null
    attr_accessor :strict
    attr_accessor :default
    attr_accessor :auto
    attr_accessor :index
    attr_accessor :unique
    attr_accessor :ignore
    attr_accessor :primary_key
    attr_accessor :surrogate_key
    attr_accessor :degenerate_key
    attr_accessor :reference
    attr_accessor :parent
    attr_accessor :debug

    def initialize(opts = {})
      self.id         = opts[:id]
      @type           = opts.fetch(:type, :integer)
      @sub_type       = opts[:sub_type]
      @null           = opts.fetch(:null, false)
      @strict         = opts.fetch(:strict, true)
      @default        = opts.fetch(:default, nil)
      @auto           = opts.fetch(:auto, false)
      @index          = opts.fetch(:index, false)
      @unique         = opts.fetch(:unique, false)
      @ignore         = opts.fetch(:ignore, false)
      @primary_key    = opts.fetch(:primary_key, false)
      @surrogate_key  = opts.fetch(:surrogate_key, false)
      @degenerate_key = opts.fetch(:degenerate_key, false)
      @reference      = opts.fetch(:reference, nil)
      @parent         = opts[:parent]
      @debug          = opts.fetch(:debug, false)

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

    def qualified_name
      (parent ? "#{parent.name}.#{name}" : name).to_sym
    end

    def reference_name
      (parent ? "#{parent.name}_#{name}" : name).to_sym
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
      [name, sql_type(primary_key), *sql_constraints, reference_constraint, sql_default].compact.join(' ')
    end

    def reference_constraint
      return if parent.temporary?
      if reference && reference.primary_key.type == type
        "REFERENCES #{reference.name}(#{reference.primary_key.name})"
      end
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

    def ==(other)
      return false unless other
      id == other.id &&
      type == other.type
    end

    def eql?(other)
      self == other
    end

    def hash
      [id, type].hash
    end

    def auto_reference
      reference && reference.primary_key.auto
    end

    def references?(other)
      if reference && other.reference && reference.id == other.reference.id
        true
      elsif parent && other.parent && parent.id == other.parent.id
        self == other
      elsif reference && other.parent && reference.id == other.parent.id
        self == other
      elsif surrogate_key || other.surrogate_key
        self == other
      else
        false
      end
    end

    def adjacent
      return unless reference
      reference.columns[id]
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
