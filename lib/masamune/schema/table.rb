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
      debug:           false
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
      @children = Set.new
      inherit_column_attributes! if inherit
    end

    def id=(id)
      @id = id.to_sym
    end

    def references=(instance)
      @references = {}
      references = (instance.is_a?(Hash) ? instance.values : instance).compact
      references.each do |reference|
        @references[reference.id] = reference
      end
    end

    def columns=(instance)
      @columns = {}
      columns = (instance.is_a?(Hash) ? instance.values : instance).compact
      raise ArgumentError, "table #{name} contains reserved columns" if columns.any? { |column| reserved_column_ids.include?(column.id) }

      initialize_surrogate_key_column! unless columns.any? { |column| column.surrogate_key }
      initialize_reference_columns!  unless columns.any? { |column| column.reference }
      columns.each do |column|
        @columns[column.name] = column.dup
        @columns[column.name].parent = self
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
      type == :stage || type == :file
    end

    def surrogate_key
      columns.values.detect { |column| column.surrogate_key }
    end

    def natural_keys
      columns.values.select { |column| column.natural_key }
    end

    def defined_columns
      columns.values
    end
    method_with_last_element :defined_columns

    def unique_constraints
      return [] if temporary?
      unique_constraints_map.map do |_, column_names|
        column_names
      end
    end

    def index_columns
      index_column_map.map do |_, column_names|
        [column_names, reverse_unique_constraints_map.key?(column_names.sort)]
      end
    end

    def unique_columns
      return {} if temporary?
      columns.select { |_, column| column.unique }
    end

    def enum_columns
      return {} if temporary?
      columns.select { |_, column| column.type == :enum }
    end

    def upsert_update_columns
      columns.values.reject { |column| reserved_column_ids.include?(column.id) || column.surrogate_key || column.natural_key || column.unique.any? || column.auto_reference || column.ignore }
    end
    method_with_last_element :upsert_update_columns

    def upsert_insert_columns
      columns.values.reject { |column| column.surrogate_key || column.auto_reference || column.ignore }
    end
    method_with_last_element :upsert_insert_columns

    def upsert_unique_columns
      columns.values.select { |column| column.unique.any? && !column.null }
    end
    method_with_last_element :upsert_unique_columns

    def reference_columns
      columns.values.select { | column| column.reference }
    end

    def foreign_key_columns
      columns.values.select { | column| column.reference && column.reference.respond_to?(:foreign_key) }
    end

    def insert_rows
      rows.select { |row| row.insert_values.any? }
    end

    def aliased_rows
      rows.select { |row| row.name }
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

    def stage_table(options = {})
      selected = options[:columns] if options[:columns]
      selected ||= options[:target].columns.values.map(&:compact_name) if options[:target]
      selected ||= []
      stage_id = [id, options[:suffix]].compact.join('_')
      @stage_tables ||= {}
      @stage_tables[[stage_id, *selected]] ||= self.class.new id: stage_id, type: :stage, store: store, columns: stage_table_columns(selected), references: references, parent: self
    end

    def as_table(table)
      table.class.new(id: id, type: :file, store: store, columns: columns.values, parent: table, inherit: true)
    end

    def shared_columns(other)
      Hash.new { |h,k| h[k] = [] }.tap do |shared|
        columns.each do |_, column|
          other.columns.each do |_, other_column|
            shared[column] << other_column if column.references?(other_column)
          end
        end
      end
    end

    def dereference_column_name(name)
      reference_name, column_name = Column::dereference_column_name(name)
      if reference = references[reference_name]
        reference.columns[column_name].dup.tap { |column| column.reference = reference }
      elsif column = columns[column_name]
        column
      end
    end

    def reserved_column_ids
      @reserved_column_ids ||= []
    end

    private

    def stage_table_columns(selected = [])
      selected = columns.keys if selected.empty?
      {}.tap do |result|
        selected.each do |name|
          column = dereference_column_name(name)
          next unless column
          next if reserved_column_ids.include?(column.id)
          next if column.surrogate_key
          result[name] = column
        end
      end
    end

    def initialize_surrogate_key_column!
      case type
      when :table
        initialize_column! id: 'uuid', type: :uuid, surrogate_key: true
      end
    end

    def initialize_reference_columns!
      references.map do |_, reference|
        if reference.denormalize
          reference.unreserved_columns.each do |_, column|
            next if column.surrogate_key
            next if column.ignore
            initialize_column! id: column.id, type: column.type, reference: reference, default: reference.default, index: true, null: reference.null, natural_key: reference.natural_key
          end
        elsif reference.foreign_key
          initialize_column! id: reference.foreign_key_name, type: reference.foreign_key_type, reference: reference, default: reference.default, index: true, null: reference.null, natural_key: reference.natural_key
        end
      end
    end

    def initialize_column!(options = {})
      column = Masamune::Schema::Column.new(options.merge(parent: self))
      @columns[column.name.to_sym] = column
    end

    def inherit_column_attributes!
      return unless parent
      columns.each do |_, column|
        parent.columns.each do |_, parent_column|
          column.index += parent_column.index if column == parent_column
        end
      end
    end

    def index_column_map
      @index_column_map ||= Hash.new { |h,k| h[k] = [] }.tap do |map|
        columns.each do |_, column|
          column.index.each do |index|
            map[index] << column.name
          end
        end
      end.sort_by { |k, v| v.length }.to_h
    end

    def unique_constraints_map
      @unique_constraints_map ||= Hash.new { |h,k| h[k] = [] }.tap do |map|
        columns.each do |_, column|
          column.unique.each do |unique|
            map[unique] << column.name
          end
        end
      end.sort_by { |k, v| v.length }.to_h
    end

    def reverse_unique_constraints_map
      @reverse_unique_constraints_map ||= unique_constraints_map.to_a.map { |k,v| [v.sort, k] }.to_h
    end
  end
end
