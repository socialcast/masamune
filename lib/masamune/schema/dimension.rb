#  The MIT License (MIT)
#
#  Copyright (c) 2014-2015, VMware, Inc. All Rights Reserved.
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
      when :one, :two, :four, :date
        'dimension'
      else
        type.to_s
      end
      parent ? [parent.suffix, suffix].compact.join('_') : suffix
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
      @ledger_table ||= self.class.new(id: id, type: :ledger, store: store, columns: ledger_table_columns, references: references.values, parent: self)
    end

    def reserved_column_ids
      case type
      when :one, :date
        [:last_modified_at]
      when :two
        [:start_at, :end_at, :version, :last_modified_at]
      when :four
        [:parent_id, :record_id, :start_at, :end_at, :version, :last_modified_at]
      when :ledger
        [:source_kind, :source_uuid, :start_at, :last_modified_at, :delta]
      else
        super
      end
    end

    private

    def ledger_table_columns
      columns.values.map do |column|
        next if column.surrogate_key
        next if reserved_column_ids.include?(column.id)

        column.dup.tap do |column_copy|
          column_copy.strict = false unless column.surrogate_key || column.natural_key || (column.reference && column.reference.surrogate_key.auto)
        end
      end.flatten
    end

    def initialize_surrogate_key_column!
      case type
      when :mini, :one, :two, :four, :ledger, :date
        initialize_column! id: 'id', type: :integer, surrogate_key: true
      end
    end

    def initialize_dimension_columns!
      # TODO assign index for load_fact
      case type
      when :one, :date
        initialize_column! id: 'last_modified_at', type: :timestamp, default: 'NOW()'
      when :two
        initialize_column! id: 'start_at', type: :timestamp, default: 'TO_TIMESTAMP(0)', index: [:start_at, :natural], unique: :natural
        initialize_column! id: 'end_at', type: :timestamp, null: true, index: :end_at
        initialize_column! id: 'version', type: :integer, default: 1, null: true
        initialize_column! id: 'last_modified_at', type: :timestamp, default: 'NOW()'
      when :four
        children << ledger_table
        # FIXME derive type from from parent
        initialize_column! id: 'parent_id', type: :integer, null: true, reference: TableReference.new(ledger_table)
        initialize_column! id: 'record_id', type: :integer, null: true, reference: TableReference.new(ledger_table)
        initialize_column! id: 'start_at', type: :timestamp, default: 'TO_TIMESTAMP(0)', index: [:start_at, :natural], unique: :natural
        initialize_column! id: 'end_at', type: :timestamp, null: true, index: :end_at
        initialize_column! id: 'version', type: :integer, default: 1, null: true
        initialize_column! id: 'last_modified_at', type: :timestamp, default: 'NOW()'
      when :ledger
        initialize_column! id: 'source_kind', type: :string, index: :natural, unique: :natural
        initialize_column! id: 'source_uuid', type: :string, index: :natural, unique: :natural
        initialize_column! id: 'start_at', type: :timestamp, index: :natural, unique: :natural
        initialize_column! id: 'last_modified_at', type: :timestamp, default: 'NOW()'
        initialize_column! id: 'delta', type: :integer
      when :stage
        if inherit
          parent.reserved_columns.each do |_, column|
            initialize_column! column.as_hash
          end
        end
      end
    end
  end
end
