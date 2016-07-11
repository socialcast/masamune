#  The MIT License (MIT)
#
#  Copyright (c) 2014-2016, VMware, Inc. All Rights Reserved.
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in
#  all copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
#  THE SOFTWARE.

require 'json'

module Masamune::Schema
  class Column
    DEFAULT_ATTRIBUTES =
    {
      id:                  nil,
      type:                :integer,
      sub_type:            nil,
      array:               false,
      values:              [],
      null:                false,
      strict:              true,
      default:             nil,
      auto:                false,
      index:               Set.new,
      unique:              Set.new,
      ignore:              false,
      sequence_offset:     1,
      surrogate_key:       false,
      natural_key:         false,
      measure:             false,
      partition:           false,
      aggregate:           nil,
      reference:           nil,
      parent:              nil,
      debug:               false
    }.freeze

    DEFAULT_ATTRIBUTES.keys.each do |attr|
      attr_accessor attr
    end

    def initialize(opts = {})
      opts.symbolize_keys!
      raise ArgumentError, 'required parameter id: missing' unless opts.key?(:id)
      DEFAULT_ATTRIBUTES.merge(opts).each do |name, value|
        public_send("#{name}=", value)
      end
    end

    def id=(id)
      @id = id.to_sym
    end

    def name
      if reference && reference.columns.include?(id)
        [reference.label, reference.name, id].compact.join('_').to_sym
      else
        id
      end
    end

    def default
      return @default unless @default.nil?
      case type
      when :uuid
        'uuid_generate_v4()'
      when :sequence
        "nextval('#{sequence_id}')"
      end
    end

    def index=(value)
      @index ||= Set.new
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
      "#{[reference.label, reference.name].compact.join('_')}.#{@id}".to_sym if reference
    end

    def compact_name
      if reference
        # XXX once columns only reference columns, this can be cleaned up
        if reference.surrogate_key && @id == reference.surrogate_key.reference_name(reference.label)
          "#{reference.id}.#{reference.surrogate_key.id}".to_sym
        else
          "#{reference.id}.#{@id}".to_sym
        end
      else
        @id
      end
    end

    def qualified_name(label = nil)
      [label, (parent ? "#{parent.name}.#{name}" : name)].compact.join('_').to_sym
    end

    def reference_name(label = nil)
      qualified_name(label).to_s.tr('.', '_').to_sym
    end

    def sql_type(for_surrogate_key = false)
      elem =
      case type
      when :integer
        for_surrogate_key ? 'SERIAL' : 'INTEGER'
      when :money
        'MONEY'
      when :string
        'VARCHAR'
      when :uuid
        'UUID'
      when :date
        'DATE'
      when :timestamp
        'TIMESTAMP'
      when :boolean
        'BOOLEAN'
      when :sequence
        'INTEGER'
      when :enum
        "#{sub_type || id}_TYPE".upcase
      when :key_value
        if parent.type == :stage && !parent.inherit
          'JSON'
        else
          'HSTORE'
        end
      when :json, :yaml
        'JSON'
      end
      array_value? ? "#{elem}[]" : elem
    end

    def hql_type(for_surrogate_key = false)
      elem =
      case type
      when :integer
        for_surrogate_key ? 'STRING' : 'INT'
      when :string, :enum, :key_value, :timestamp
        'STRING'
      else
        sql_type
      end
      array_value? ? "ARRAY<#{elem}>" : elem
    end

    def sql_value(value)
      return value if sql_function?(value)
      return 'NULL' if value == :null
      case type
      when :boolean
        value ? 'TRUE' : 'FALSE'
      when :string
        "'#{value}'"
      when :enum
        "'#{value}'::#{sql_type}"
      else
        value
      end
    end

    def csv_value(value)
      return value if sql_function?(value)
      return csv_array(value) if array_value?
      return nil if value.nil?
      case type
      when :boolean
        if value
          'TRUE'
        else
          hive_encoding? ? nil : 'FALSE'
        end
      when :yaml
        value.to_hash.to_yaml
      when :json, :key_value
        value.to_hash.to_json
      when :date
        value.to_s
      when :timestamp
        value.to_time.utc.iso8601(3)
      when :string
        value.empty? ? nil : value
      else
        value
      end
    rescue
      raise ArgumentError, "Could not coerce '#{value}' into :#{type} for column '#{name}'"
    end

    def ruby_value(value, recursive = true)
      value = nil if null_value?(value)
      return value if sql_function?(value)
      return ruby_array(value) if recursive && array_value?
      case type
      when :boolean
        case value
        when false, 0, '0', "'0'", /\Afalse\z/i
          false
        when true, 1, '1', "'1'", /\Atrue\z/i
          true
        end
      when :date
        case value
        when Date
          value
        when String
          Date.parse(value.to_s)
        when nil
          nil
        end
      when :timestamp
        case value
        when Time
          value
        when Date, DateTime
          value.to_time
        when String
          if value.blank?
            nil
          elsif value =~ /\A\d+\z/
            Time.at(value.to_i)
          else
            Time.parse(value)
          end
        when Integer
          Time.at(value)
        when nil
          nil
        end
      when :integer
        value.blank? ? nil : value.to_i
      when :yaml
        case value
        when Hash
          value
        when String
          YAML.load(value)
        when nil
          {}
        end
      when :json
        case value
        when Hash
          value
        when String
          JSON.parse(value)
        when nil
          {}
        end
      when :string
        value.blank? ? nil : value.to_s
      else
        value
      end
    rescue
      raise ArgumentError, "Could not coerce '#{value}' into :#{type} for column '#{name}'"
    end

    def default_ruby_value
      return [] if array_value?
      return HashWithIndifferentAccess.new { |h, k| h[k] = HashWithIndifferentAccess.new(&h.default_proc) } if hash_value?
      case type
      when :date
        Date.new(0)
      when :timestamp
        Time.new(0)
      end
    end

    def aggregate_value
      return qualified_name unless aggregate
      case aggregate
      when :min
        "MIN(#{qualified_name})"
      when :max
        "MAX(#{qualified_name})"
      when :sum
        "SUM(#{qualified_name})"
      when :average
        "AVG(#{qualified_name})"
      end
    end

    def null_value?(value)
      if type == :json || array_value?
        return true if value == 'NULL'
      end
      return false unless value
      if hive_encoding?
        value.to_s == '\N'
      else
        false
      end
    end

    def hive_encoding?
      if parent && parent.store
        parent.store.type == :hive
      else
        false
      end
    end

    def sql_function?(value)
      value =~ /\(\)\Z/
    end

    def array_value?
      (array || (reference && reference.respond_to?(:multiple) && reference.multiple)) == true
    end

    def hash_value?
      [:key_value, :yaml, :json].include?(type)
    end

    def as_psql
      [name, sql_type(surrogate_key), *sql_constraints, sql_default].compact.join(' ')
    end

    def as_hql
      [name, hql_type(surrogate_key)].compact.join(' ')
    end

    def as_hash
      { id: id }.tap do |hash|
        DEFAULT_ATTRIBUTES.keys.each do |attr|
          hash[attr] = public_send(attr)
        end
      end
    end

    class << self
      def dereference_column_name(name)
        return unless name
        if name.to_s =~ /\./
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

    # XXX hack to work around columns not being able to reference columns
    def references?(other)
      return false unless other
      if reference && other.reference && reference.id == other.reference.id
        true
      elsif parent && other.parent && parent.id == other.parent.id
        self == other
      elsif parent && other.parent && other.parent.parent && parent.id == other.parent.parent.id
        self == other
      elsif reference && other.parent && reference.id == other.parent.id
        self == other
      elsif natural_key || other.natural_key
        self == other
      else
        false
      end
    end

    def degenerate?
      reference && reference.respond_to?(:degenerate) && reference.degenerate
    end

    def adjacent
      return unless reference
      reference.columns[id]
    end

    def sequence_id
      "#{reference_name}_seq" if type == :sequence
    end

    def required_value?
      return false if reference && (reference.null || !reference.default.nil?)
      return false if null || !default.nil?
      return false unless strict
      true
    end

    private

    def sql_constraints
      [].tap do |constraints|
        constraints << 'NOT NULL' unless null || surrogate_key || !strict || parent.temporary? || degenerate?
      end
    end

    def sql_default
      return if default.nil?
      return unless strict
      "DEFAULT #{sql_value(default)}"
    end

    def ruby_array(value)
      case value
      when Array
        value.map { |elem| ruby_value(elem, false) }
      when String
        Array.wrap(JSON.load(value)).map { |elem| ruby_value(elem, false) }
      when nil
        []
      end
    end

    def csv_array(value)
      case value
      when Array
        ruby_value(value).to_json
      when nil
        [].to_json
      else
        [ruby_value(value, false)].to_json
      end
    end
  end
end
