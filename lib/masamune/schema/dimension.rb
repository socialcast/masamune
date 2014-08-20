module Masamune::Schema
  class Dimension < Table
    def initialize(o)
      super
      initialize_dimension_columns!
    end

    def name
      case type
      when :mini
        "#{id}_type"
      when :stage
        parent ? "#{parent.name}_stage" : "#{id}_stage"
      when :one, :two, :four
        "#{id}_dimension"
      when :ledger
        parent ? "#{parent.name}_ledger" : "#{id}_ledger"
      end
    end

    def start_key
      columns.values.detect { |column| column.id == :start_at }
    end

    def end_key
      columns.values.detect { |column| column.id == :end_at }
    end

    def version_key
      columns.values.detect { |column| column.id == :version }
    end

    def upsert_unique_columns
      columns.values.select { |column| [:source_kind, :start_at].include?(column.id) || column.surrogate_key || column.unique }
    end
    method_with_last_element :upsert_unique_columns

    def consolidated_window(*extra)
      (columns.values.select { |column| extra.delete(column.name) || column.surrogate_key }.map(&:name) + extra).uniq
    end

    def consolidated_columns
      columns.reject { |_, column| [:end_at, :version, :last_modified_at].include?(column.id) || column.primary_key }
    end
    method_with_last_element :consolidated_columns

    def consolidated_values(window: nil)
      consolidated_columns.reject { |id, _| [:parent_uuid, :record_uuid].include?(id) }.map do |_, column, _|
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
      consolidated_columns.reject { |id, column| [:parent_uuid, :record_uuid, :start_at].include?(id) || column.null }
    end

    def ledger_table
      @ledger_table ||= self.class.new(id: id, type: :ledger, columns: ledger_table_columns, references: references.values, parent: self)
    end

    private

    def ledger_table_columns
      columns.values.map do |column|
        next if column.primary_key
        next if reserved_column_ids.include?(column.id)

        if column.type == :key_value
          column_now, column_was = column.dup, column.dup
          column_now.id, column_was.id = "#{column.id}_now", "#{column.id}_was"
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
        initialize_column! id: 'id', type: :integer, primary_key: true
      when :one, :two, :four, :ledger
        initialize_column! id: 'uuid', type: :uuid, primary_key: true
      end
    end

    def initialize_dimension_columns!
      case type
      when :one
        initialize_column! id: 'last_modified_at', type: :timestamp, default: 'NOW()'
      when :two
        initialize_column! id: 'start_at', type: :timestamp, default: 'TO_TIMESTAMP(0)', index: true
        initialize_column! id: 'end_at', type: :timestamp, null: true, index: true
        initialize_column! id: 'version', type: :integer, default: 1, null: true
        initialize_column! id: 'last_modified_at', type: :timestamp, default: 'NOW()'
      when :four
        children << ledger_table
        initialize_column! id: 'parent_uuid', type: :uuid, null: true, reference: ledger_table
        initialize_column! id: 'record_uuid', type: :uuid, null: true, reference: ledger_table
        initialize_column! id: 'start_at', type: :timestamp, default: 'TO_TIMESTAMP(0)', index: true
        initialize_column! id: 'end_at', type: :timestamp, null: true, index: true
        initialize_column! id: 'version', type: :integer, default: 1, null: true
        initialize_column! id: 'last_modified_at', type: :timestamp, default: 'NOW()'
      when :ledger
        initialize_column! id: 'source_kind', type: :string, null: true
        initialize_column! id: 'source_uuid', type: :string, null: true
        initialize_column! id: 'start_at', type: :timestamp, index: true
        initialize_column! id: 'last_modified_at', type: :timestamp, default: 'NOW()'
        initialize_column! id: 'delta', type: :integer
      end
    end

    def reserved_column_ids
      @reserved_column_ids ||=
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
