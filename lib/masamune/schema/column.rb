require 'json'

module Masamune::Schema
  class Column
    DEFAULT_ATTRIBUTES =
    {
      id:                  nil,
      type:                :integer,
      sub_type:            nil,
      values:              [],
      null:                false,
      strict:              true,
      default:             nil,
      auto:                false,
      index:               Set.new,
      unique:              Set.new,
      ignore:              false,
      surrogate_key:       false,
      natural_key:         false,
      degenerate_key:      false,
      reference:           nil,
      parent:              nil,
      debug:               false
    }

    DEFAULT_ATTRIBUTES.keys.each do |attr|
      attr_accessor attr
    end

    def initialize(opts = {})
      opts.symbolize_keys!
      raise ArgumentError, 'required parameter id: missing' unless opts.key?(:id)
      DEFAULT_ATTRIBUTES.merge(opts).each do |name, value|
        public_send("#{name}=", value)
      end

      initialize_default_attributes!
    end

    def id=(id)
      @id = id.to_sym
    end

    def name
      if reference && reference.columns.include?(@id)
        [reference.label, reference.name, @id].compact.join('_').to_sym
      else
        @id
      end
    end

    def index=(value)
      @index ||= Set.new
      @index.clear
      @index +=
      case value
      when true
        [id]
      when false
        []
      when String, Symbol
        [value.to_sym]
      when Array, Set
        value.map(&:to_sym)
      else
        raise ArgumentError
      end
    end

    def unique=(value)
      @unique ||= Set.new
      @unique.clear
      @unique +=
      case value
      when true
        [id]
      when false
        []
      when String, Symbol
        [value.to_sym]
      when Array, Set
        value.map(&:to_sym)
      else
        raise ArgumentError
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

    def qualified_name(label = nil)
      [label, (parent ? "#{parent.name}.#{name}" : name)].compact.join('_').to_sym
    end

    def reference_name(label = nil)
      qualified_name(label).to_s.gsub(/\./, '_').to_sym
    end

    def sql_type(for_surrogate_key = false)
      case type
      when :integer
        for_surrogate_key ? 'SERIAL' : 'INTEGER'
      when :money
        'MONEY'
      when :string
        'VARCHAR'
      when :uuid
        'UUID'
      when :timestamp
        'TIMESTAMP'
      when :boolean
        'BOOLEAN'
      when :enum
        "#{sub_type}_TYPE".upcase
      when :key_value
        parent.type == :file ? 'JSON' : 'HSTORE'
      when :json, :yaml
        'JSON'
      end
    end

    def sql_value(value)
      return value if sql_function?(value)
      case type
      when :boolean
        value ? 'TRUE' : 'FALSE'
      when :string, :enum
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
      when :yaml
        value.to_h.to_yaml
      when :json, :key_value
        value.to_h.to_json
      else
        value
      end
    end

    def ruby_value(value)
      value = nil if null_value?(value)
      return value if sql_function?(value)
      case type
      when :boolean
        case value
        when false, 0, '0', "'0'", /\Afalse\z/i
          false
        when true, 1, '1', "'1'", /\Atrue\z/i
          true
        end
      when :integer
        value.nil? ? nil : value.to_i
      when :yaml
        case value
        when Hash
          value
        when String
          ruby_key_value(YAML.load(value))
        when nil
          {}
        end
      when :json
        case value
        when Hash
          value
        when String
          ruby_key_value(JSON.load(value))
        when nil
          {}
        end
      else
        value
      end
    end

    def null_value?(value)
      return false unless parent
      case parent.kind
      when :hql
        value == '\N'
      when :psql
        false
      end
    end

    def sql_function?(value)
      value =~ /\(\)\Z/
    end

    def as_psql
      [name, sql_type(surrogate_key), *sql_constraints, reference_constraint, sql_default].compact.join(' ')
    end

    def reference_constraint
      return if parent.temporary?
      if reference && reference.surrogate_key.type == type
        "REFERENCES #{reference.name}(#{reference.surrogate_key.name})"
      end
    end

    class << self
      def dereference_column_name(name)
        return unless name
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
      typecast?(other.type) &&
      (!reference || reference.id == other.reference.try(:id) || reference.id == other.parent.try(:id)) &&
      (!other.reference || other.reference.id == reference.try(:id) || other.reference.id == parent.try(:id))
    end

    def eql?(other)
      self == other
    end

    def hash
      [id, type].hash
    end

    def typecast?(other_type)
      return true if type == other_type
      case [type, other_type]
      when [:key_value, :yaml]
        true
      when [:key_value, :json]
        true
      when [:yaml, :json]
        true
      else
        false
      end
    end

    def auto_reference
      reference && reference.surrogate_key.auto && !reference.insert
    end

    def references?(other)
      return false unless other
      if reference && other.reference && reference.id == other.reference.id
        true
      elsif parent && other.parent && parent.id == other.parent.id
        self == other
      elsif reference && other.parent && reference.id == other.parent.id
        self == other
      elsif natural_key || other.natural_key
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
        constraints << 'NOT NULL' unless null || surrogate_key || !strict || parent.temporary?
        constraints << 'PRIMARY KEY' if surrogate_key
      end
    end

    def sql_default
      "DEFAULT #{sql_value(default)}" unless default.nil?
    end

    def initialize_default_attributes!
      self.default = 'uuid_generate_v4()' if surrogate_key && type == :uuid
      self.unique = 'natural' if natural_key
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
