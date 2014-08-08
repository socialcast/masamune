module Masamune::Schema
  class Dimension < Table
    def initialize(o)
      super
      initialize_dimension_columns!
    end

    def table_name
      case type
      when :mini
        "#{name}_type"
      when :stage
        parent ? "#{parent.table_name}_stage" : "#{name}_stage"
      when :one, :two, :four
        "#{name}_dimension"
      when :ledger
        parent ? "#{parent.table_name}_ledger" : "#{name}_ledger"
      end
    end

    def consolidated_window(*extra)
      (columns.values.select { |column| extra.delete(column.name) || column.surrogate_key }.map(&:name) + extra).uniq
    end

    def consolidated_columns
      columns.reject { |_, column| [:end_at, :version, :last_modified_at].include?(column.name) || column.primary_key }
    end
    method_with_last_element :consolidated_columns

    def consolidated_values(window: nil)
      consolidated_columns.reject { |name, _| [:parent_uuid, :record_uuid].include?(name) }.map do |_, column, _|
        if column.surrogate_key || column.name == :start_at
          "#{column.name} AS #{column.name}"
        elsif column.type == :key_value
          "hstore_merge(#{column.name}_now) OVER #{window} - hstore_merge(#{column.name}_was) OVER #{window} AS #{column.name}"
        else
          "COALESCE(#{column.name}, FIRST_VALUE(#{column.name}) OVER #{window}) AS #{column.name}"
        end
      end
    end
    method_with_last_element :consolidated_values

    def consolidated_constraints
      consolidated_columns.reject { |name, column| [:parent_uuid, :record_uuid, :start_at].include?(name) || column.null }
    end

    def ledger_table
      @ledger_table ||= self.class.new(name: name, type: :ledger, columns: ledger_table_columns, references: references.values, parent: self)
    end

    private

    def ledger_table_columns
      columns.values.map do |column|
        next if column.primary_key
        next if reserved_column_names.include?(column.name)

        if column.type == :key_value
          column_now, column_was = column.dup, column.dup
          column_now.name, column_was.name = "#{column.name}_now", "#{column.name}_was"
          column_now.strict, column_was.strict = false, false
          [column_now, column_was]
        else
          column.dup.tap do |column_copy|
            column_copy.strict = false unless column.primary_key || column.surrogate_key
          end
        end
      end.flatten
    end

    def initialize_primary_key_column!
      case type
      when :mini
        initialize_column! name: 'id', type: :integer, primary_key: true
      when :one, :two, :four, :ledger
        initialize_column! name: 'uuid', type: :uuid, primary_key: true
      end
    end

    def initialize_dimension_columns!
      case type
      when :one
        initialize_column! name: 'last_modified_at', type: :timestamp, default: 'NOW()'
      when :two
        initialize_column! name: 'start_at', type: :timestamp, default: 'TO_TIMESTAMP(0)', index: true
        initialize_column! name: 'end_at', type: :timestamp, null: true, index: true
        initialize_column! name: 'version', type: :integer, default: 1, null: true
        initialize_column! name: 'last_modified_at', type: :timestamp, default: 'NOW()'
      when :four
        children << ledger_table
        initialize_column! name: 'parent_uuid', type: :uuid, null: true, reference: ledger_table
        initialize_column! name: 'record_uuid', type: :uuid, null: true, reference: ledger_table
        initialize_column! name: 'start_at', type: :timestamp, default: 'TO_TIMESTAMP(0)', index: true
        initialize_column! name: 'end_at', type: :timestamp, null: true, index: true
        initialize_column! name: 'version', type: :integer, default: 1, null: true
        initialize_column! name: 'last_modified_at', type: :timestamp, default: 'NOW()'
      when :ledger
        initialize_column! name: 'source_kind', type: :string, null: true
        initialize_column! name: 'source_uuid', type: :string, null: true
        initialize_column! name: 'start_at', type: :timestamp, index: true
        initialize_column! name: 'last_modified_at', type: :timestamp, default: 'NOW()'
        initialize_column! name: 'delta', type: :integer
      end
    end

    def reserved_column_names
      @reserved_column_names ||=
      case type
      when :one
        [:last_modified_at]
      when :two
        [:start_at, :end_at, :version, :last_modified_at]
      when :four
        [:parent_uuid, :record_uuid, :start_at, :end_at, :version, :last_modified_at]
      when :ledger
        [:source_kind, :source_uuid, :start_at, :last_modified_at, :delta]
      else
        []
      end
    end
  end
end
