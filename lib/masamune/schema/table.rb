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

require 'masamune/last_element'

module Masamune::Schema
  class Table
    include Masamune::LastElement

    attr_reader :children

    DEFAULT_ATTRIBUTES =
    {
      id:              nil,
      name:            nil,
      type:            :table,
      store:           nil,
      parent:          nil,
      suffix:          nil,
      implicit:        false,
      references:      {},
      columns:         {},
      rows:            [],
      inherit:         false,
      debug:           false,
      properties:      {}
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
      @children = Set.new
    end

    def id=(id)
      @id = id.to_sym
    end

    def references=(instance)
      @references = {}
      references = (instance.is_a?(Hash) ? instance.values : instance).compact
      references.each do |reference|
        raise ArgumentError, "table #{name} contains invalid table references" unless reference.is_a?(TableReference)
        @references[reference.id] = reference
      end
    end

    def columns=(instance)
      @columns = {}
      columns = (instance.is_a?(Hash) ? instance.values : instance).compact
      raise ArgumentError, "table #{name} contains reserved columns" if columns.any? { |column| reserved_column_ids.include?(column.id) }

      initialize_surrogate_key_column! unless columns.any?(&:surrogate_key)
      initialize_reference_columns! unless columns.any?(&:reference)
      columns.each do |column|
        initialize_column!(column)
      end
    end

    def rows=(rows)
      @rows = []
      rows.each do |row|
        @rows << row.dup
        @rows.last.parent = self
      end
    end

    def name
      @name || [id, suffix].compact.join('_')
    end

    def suffix
      ((parent ? parent.suffix.split('_') : []) + [type.to_s, @suffix]).compact.uniq.join('_')
    end

    def temporary?
      type == :stage
    end

    def surrogate_key
      columns.values.detect(&:surrogate_key)
    end

    def primary_keys
      [*auto_surrogate_keys, surrogate_key].compact
    end

    def natural_keys
      columns.values.select(&:natural_key)
    end

    def defined_columns
      columns.values.reject(&:partition)
    end
    method_with_last_element :defined_columns

    def unique_constraints
      return [] if temporary?
      unique_constraints_map.map do |_, column_names|
        [column_names, short_md5(column_names.to_a)]
      end.uniq
    end

    def index_columns
      index_column_map.map do |_, column_names|
        unique_index = reverse_unique_constraints_map.key?(column_names.sort)
        [column_names, unique_index, short_md5(column_names.to_a)]
      end.uniq
    end

    def unique_columns
      return {} if temporary?
      columns.select { |_, column| column.unique }
    end

    def enum_columns
      return {} if temporary?
      columns.select { |_, column| column.type == :enum }
    end

    def sequence_columns
      return {} if temporary?
      columns.select { |_, column| column.reference.nil? && column.type == :sequence }
    end

    def reference_columns
      columns.values.select(&:reference)
    end

    def foreign_key_columns
      columns.values.select { |column| !column.degenerate? && column.reference && column.reference.foreign_key }
    end

    def partitions
      columns.select { |_, column| column.partition }
    end

    def insert_rows
      rows.select { |row| row.insert_values.any? }
    end

    def aliased_rows
      rows.select(&:name)
    end

    def insert_references
      references.select { |_, reference| reference.insert }
    end

    def reserved_columns
      columns.select { |_, column| reserved_column_ids.include?(column.id) }
    end

    def unreserved_columns
      columns.reject { |_, column| reserved_column_ids.include?(column.id) }
    end

    def denormalized_columns
      return to_enum(__method__).to_a.flatten.compact unless block_given?
      columns.map do |_, column|
        next if column.surrogate_key || column.ignore
        if column.reference && column.reference.natural_keys.any?
          column.reference.natural_keys.each do |join_column|
            next if join_column.reference && join_column.natural_key
            yield [column.reference, join_column]
          end
        elsif column.reference && column.reference.denormalized_columns.any?
          column.reference.denormalized_columns.each do |join_column|
            yield [column.reference, join_column]
          end
        else
          yield [nil, column]
        end
      end
    end

    def denormalized_column_names
      return to_enum(__method__).to_a unless block_given?
      denormalized_columns do |reference, column|
        if column.parent == self
          yield column.name.to_s
        elsif reference
          yield [reference.id, column.name].compact.join('.')
        else
          yield [column.parent.id, column.name].compact.join('.')
        end
      end
    end

    def stage_table(options = {})
      selected = options[:columns] if options[:columns]
      selected ||= options[:target].columns.values.map(&:compact_name) if options[:target]
      selected ||= []
      stage_id = [id, options[:suffix]].compact.join('_')
      parent = options[:table] ? options[:table] : self
      type = options[:type] ? options[:type] : :stage
      @stage_tables ||= {}
      @stage_tables[options] ||= parent.class.new id: stage_id, type: type, store: store, columns: stage_table_columns(parent, selected, options.fetch(:inherit, true)), references: stage_table_references(parent, selected), parent: parent, inherit: options.fetch(:inherit, true)
    end

    def shared_columns(other)
      Hash.new { |h, k| h[k] = [] }.tap do |shared|
        columns.each do |_, column|
          other.columns.each do |_, other_column|
            shared[column] << other_column if column.references?(other_column)
          end
        end
      end
    end

    def dereference_column_name(name)
      reference_name, column_name = Column.dereference_column_name(name)
      reference = references[reference_name]
      if reference
        column = reference.columns[column_name]
        dereference_column(column.dup, reference) if column
      elsif columns[column_name]
        columns[column_name]
      end
    end

    def dereference_column(column, reference)
      column.surrogate_key = false
      column.reference = reference
      column
    end

    def reserved_column_ids
      inherit ? parent.reserved_column_ids : []
    end

    # NOTE: postgres bigint is 8 bytes long
    def lock_id
      Integer('0x' + Digest::MD5.hexdigest(name)) % (1 << 63)
    end

    def auto_surrogate_keys
      columns.values.select { |column| column.reference && column.reference.surrogate_key.auto }.uniq.compact
    end

    def foreign_key_constraints
      return [] if temporary?
      foreign_key_columns.map do |column|
        if column.reference.auto_surrogate_keys == auto_surrogate_keys
          column_names = [*column.reference.auto_surrogate_keys.map(&:name), column.name].compact
          reference_column_names = [*column.reference.auto_surrogate_keys.map(&:name), column.reference.surrogate_key.name].compact
        else
          column_names = [column.name]
          reference_column_names = [column.reference.surrogate_key.name]
        end
        [short_md5(column_names), column_names, column.reference.name, reference_column_names]
      end.compact
    end

    private

    def stage_table_columns(parent, selected = [], inherit = true)
      selected = columns.keys if selected.empty?
      {}.tap do |result|
        selected.each do |name|
          column = dereference_column_name(name)
          next unless column
          next if inherit && parent.reserved_column_ids.include?(column.id)
          next if column.parent == self && column.surrogate_key
          result[name] = column
        end
      end
    end

    def stage_table_references(_parent, selected = [])
      selected = references.keys if selected.empty?
      {}.tap do |result|
        selected.each do |name|
          column = dereference_column_name(name)
          next unless column
          next if column.parent == self
          result[name] = column.reference
        end
      end
    end

    def initialize_surrogate_key_column!
      case type
      when :table
        initialize_column! id: 'id', type: :integer, surrogate_key: true
      end
    end

    def initialize_reference_columns!
      references.map do |_, reference|
        if reference.denormalize
          reference.unreserved_columns.each do |_, column|
            next if column.surrogate_key
            next if column.ignore
            initialize_column! id: column.id, type: column.type, reference: reference, default: reference.default, null: reference.null, natural_key: reference.natural_key
          end
        elsif reference.foreign_key
          # FIXME: column.reference should point to reference.surrogate_key, only allow column references to Columns
          initialize_column! id: reference.foreign_key_name, type: reference.foreign_key_type, reference: reference, default: reference.default, null: reference.null, natural_key: reference.natural_key
        end
      end
    end

    def initialize_column!(column_or_options)
      column = column_or_options.is_a?(Column) ? column_or_options.dup : Column.new(column_or_options.merge(parent: self))
      column_key = column.name.to_sym
      @columns[column_key] = column
      @columns[column_key].parent = self
      @columns[column_key].index += [column_key, :natural] if column.natural_key
      @columns[column_key].unique << :natural if column.natural_key
    end

    def index_column_map
      @index_column_map ||= begin
        map = Hash.new { |h, k| h[k] = [] }
        columns.each do |_, column|
          column.index.each do |index|
            map[index] << column.name
            map[index].uniq!
          end
        end
        Hash[map.sort_by { |k, v| [v.length, k.to_s] }]
      end
    end

    def unique_constraints_map
      @unique_constraints_map ||= begin
        map = Hash.new { |h, k| h[k] = [] }
        columns.each do |_, column|
          next if column.auto_reference
          column.unique.each do |unique|
            map[unique] += auto_surrogate_keys.map(&:name)
            map[unique] << column.name
            map[unique].uniq!
          end
        end unless temporary?
        Hash[map.sort_by { |k, v| [v.length, k.to_s] }]
      end
    end

    def reverse_unique_constraints_map
      @reverse_unique_constraints_map ||= Hash[unique_constraints_map.to_a.map { |k, v| [v.sort, k] }]
    end

    def short_md5(*a)
      Digest::MD5.hexdigest(a.compact.sort.uniq.join('_'))[0..6]
    end
  end
end
