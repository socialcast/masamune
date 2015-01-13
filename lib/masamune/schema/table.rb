module Masamune::Schema
  class Table
    include Masamune::LastElement

    attr_reader :children

    DEFAULT_ATTRIBUTES =
    {
      id:              nil,
      type:            :table,
      parent:          nil,
      suffix:          nil,
      references:      {},
      headers:         true,
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

    def kind
      :psql
    end

    def format
      :csv
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
      initialize_foreign_key_columns!
      columns.each do |column|
        column.parent = self
        @columns[column.name] = column
      end
    end

    def rows=(rows)
      @rows = []
      rows.each do |row|
        row.parent = self
        @rows << row
      end
    end

    def name
      "#{id}_#{suffix}"
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

    def foreign_key_columns
      columns.values.select { | column| column.reference }
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

    def stage_table(suffix = nil)
      stage_id = [id, suffix].compact.join('_')
      @stage_tables ||= {}
      @stage_tables[stage_id] ||= self.class.new id: stage_id, type: :stage, columns: stage_table_columns, parent: self
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

    def as_psql(extra = {})
      Masamune::Template.render_to_string(table_template, extra.merge(table: self))
    end

    def select_columns(selected_columns = [])
      return columns.values unless selected_columns.any?
      [].tap do |result|
        selected_columns.map(&:to_sym).each do |name|
          reference_name, column_name = Column::dereference_column_name(name)
          if reference = references[reference_name]
            if reference.columns[column_name]
              result << reference.columns[column_name].dup.tap { |column| column.reference = reference }
            end
          elsif columns[column_name]
            result << columns[column_name]
          end
        end
      end
    end

    def as_file(selected_columns = [])
      File.new(id: id, columns: select_columns(selected_columns), headers: headers)
    end

    protected

    def reserved_column_ids
      @reserved_column_ids ||= []
    end

    private

    def stage_table_columns
      unreserved_columns.map { |_, column| column.dup }
    end

    def initialize_surrogate_key_column!
      case type
      when :table
        initialize_column! id: 'uuid', type: :uuid, surrogate_key: true
      end
    end

    def initialize_foreign_key_columns!
      references.map do |_, reference|
        initialize_column! id: reference.foreign_key_name, type: reference.foreign_key_type, reference: reference, default: reference.default, index: true, null: reference.null, natural_key: reference.natural_key
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

    def table_template
      @table_template ||= ::File.expand_path(::File.join(__FILE__, '..', 'table.psql.erb'))
    end
  end
end
