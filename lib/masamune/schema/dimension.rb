module Masamune::Schema
  class Dimension < Table
    def initialize(opts = {})
      super
      initialize_dimension_columns!
    end

    def suffix
      suffix = case type
      when :mini
        'type'
      when :one, :two, :four
        'dimension'
      else
        type.to_s
      end
      parent ? "#{parent.suffix}_#{suffix}" : suffix
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

    def ledger_table
      @ledger_table ||= self.class.new(id: id, type: :ledger, columns: ledger_table_columns, references: references.values, parent: self)
    end

    protected

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
      when :stage
        parent.reserved_column_ids
      else
        super
      end
    end

    private

    def ledger_table_columns
      columns.values.map do |column|
        next if column.surrogate_key
        next if reserved_column_ids.include?(column.id)

        if column.type == :key_value
          column_now, column_was = column.dup, column.dup
          column_now.id, column_was.id = "#{column.id}_now", "#{column.id}_was"
          column_now.strict, column_was.strict = false, false
          [column_now, column_was]
        else
          column.dup.tap do |column_copy|
            column_copy.strict = false unless column.surrogate_key || column.natural_key
          end
        end
      end.flatten
    end

    def initialize_surrogate_key_column!
      case type
      when :mini
        initialize_column! id: 'id', type: :integer, surrogate_key: true
      when :one, :two, :four, :ledger
        initialize_column! id: 'uuid', type: :uuid, surrogate_key: true
      end
    end

    def initialize_dimension_columns!
      case type
      when :one
        initialize_column! id: 'last_modified_at', type: :timestamp, default: 'NOW()'
      when :two
        initialize_column! id: 'start_at', type: :timestamp, default: 'TO_TIMESTAMP(0)', index: true, unique: 'natural'
        initialize_column! id: 'end_at', type: :timestamp, null: true, index: true
        initialize_column! id: 'version', type: :integer, default: 1, null: true, index: true
        initialize_column! id: 'last_modified_at', type: :timestamp, default: 'NOW()'
      when :four
        children << ledger_table
        initialize_column! id: 'parent_uuid', type: :uuid, null: true, reference: ledger_table
        initialize_column! id: 'record_uuid', type: :uuid, null: true, reference: ledger_table
        initialize_column! id: 'start_at', type: :timestamp, default: 'TO_TIMESTAMP(0)', index: true, unique: 'natural'
        initialize_column! id: 'end_at', type: :timestamp, null: true, index: true
        initialize_column! id: 'version', type: :integer, default: 1, null: true, index: true
        initialize_column! id: 'last_modified_at', type: :timestamp, default: 'NOW()'
      when :ledger
        initialize_column! id: 'source_kind', type: :string, unique: 'natural'
        initialize_column! id: 'source_uuid', type: :string, unique: 'natural'
        initialize_column! id: 'start_at', type: :timestamp, index: true, unique: 'natural'
        initialize_column! id: 'last_modified_at', type: :timestamp, default: 'NOW()'
        initialize_column! id: 'delta', type: :integer
      when :stage
        parent.reserved_columns.each do |_, column|
          initialize_column! column.as_hash
        end
      end
    end
  end
end
