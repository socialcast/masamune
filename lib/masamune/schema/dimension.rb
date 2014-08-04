module Masamune::Schema
  class Dimension
    include Masamune::LastElement

    attr_accessor :name
    attr_accessor :type
    attr_accessor :ledger
    attr_accessor :references
    attr_accessor :columns
    attr_accessor :rows
    attr_accessor :insert
    attr_accessor :ledger_table
    attr_accessor :debug

    def initialize(name: name, type: :two, ledger: false, references: [], columns: [], rows: [], insert: false, debug: false)
      @name   = name.to_sym
      @type   = type
      @ledger = ledger
      @rows   = rows
      @insert = insert
      @debug  = debug

      @references = {}
      references.each do |reference|
        @references[reference.name] = reference
      end

      raise ArgumentError, "dimension #{name} contains reserved columns" if columns.any? { |column| reserved_column_names.include?(column.name) }

      @columns = {}
      initialize_primary_key_column! unless columns.any? { |column| column.primary_key }
      initialize_foreign_key_columns!
      columns.each do |column|
        @columns[column.name] = column
        @columns[column.name].parent = self
      end
      initialize_ledger_table!
      initialize_dimension_columns!

      @rows.each { |row| row.reference = self }
    end

    def temporary?
      type == :stage
    end

    def table_name
      case type
      when :mini
        "#{name}_type"
      when :stage
        "#{name}_stage"
      when :two
        "#{name}_dimension"
      when :ledger
        "#{name}_dimension_ledger"
      when :ledger_stage
        "#{name}_dimension_ledger_stage"
      end
    end

    def primary_key
      columns.values.detect { |column| column.primary_key }
    end

    def surrogate_keys
      columns.values.select { |column| column.surrogate_key }
    end

    def foreign_key_name
      "#{table_name}_#{primary_key.name}"
    end

    def defined_columns
      columns.values
    end
    method_with_last_element :defined_columns

    def index_columns
      return [] if temporary?
      indices = columns.select { |_, column| column.index }.lazy
      indices = indices.group_by { |_, column| column.index == true ? column.name : column.index }.lazy
      indices = indices.map { |_, index_and_columns| index_and_columns.map(&:last) }.lazy
      indices.map do |columns|
        [columns.map(&:name), columns.all? { |column| column.unique }]
      end
    end

    def unique_columns
      return {} if temporary?
      columns.select { |_, column| column.unique } || {}
    end

    def foreign_key_columns
      references.map do |_, dimension|
        name = dimension.primary_key.name
        type = dimension.primary_key.type
        Masamune::Schema::Column.new(name: name, type: type, reference: dimension, default: dimension.default_foreign_key_row)
      end
    end

    def insert_columns
      columns.map do |_, column|
        if reference = column.reference
          reference.foreign_key_name
        else
          column.name
        end
      end
    end

    def upsert_update_columns
      columns.values.reject { |column| reserved_column_names.include?(column.name) || column.primary_key || column.surrogate_key }
    end
    method_with_last_element :upsert_update_columns

    def upsert_insert_columns
      columns.values.reject { |column| column.primary_key }
    end
    method_with_last_element :upsert_insert_columns

    # TODO optionally enable source_uuid
    def upsert_unique_columns
      columns.values.select { |column| [:source_kind, :start_at].include?(column.name) || column.surrogate_key }
    end
    method_with_last_element :upsert_unique_columns

    def default_foreign_key_row
      rows.select { |row| row.default }.first.try(:name)
    end

    def insert_rows
      rows.select { |row| row.insert_values.any? }
    end

    def insert_values
      columns.map do |_, column|
        if reference = column.reference
          select = "(SELECT #{reference.primary_key.name} FROM #{reference.table_name} WHERE #{column.foreign_key_name} = #{column.name})"
          if reference.default_value
            "COALESCE(#{select}, #{reference.default_value})"
          else
            select
          end
        elsif column.type == :json
          "json_to_hstore(#{column.name})"
        else
          column.name.to_s
        end
      end
    end
    method_with_last_element :insert_values

    def default_value
      rows.detect { |row| row.default }.try(:name)
    end

    def consolidated_window(*extra)
      columns.values.select { |column| extra.delete(column.name) || column.surrogate_key }.map(&:name) + extra
    end

    def consolidated_columns
      columns.reject { |_, column| [:end_at, :version, :last_modified_at].include?(column.name) || column.primary_key }
    end
    method_with_last_element :consolidated_columns

    def consolidated_values(window: nil)
      x = consolidated_columns.reject { |name, _| [:parent_uuid, :record_uuid].include?(name) }.map do |name, column, _|
        if column.surrogate_key || column.name == :start_at
          [name, column.name]
        # FIXME stage is cast to json unnecessarily
        elsif column.type == :key_value || column.type == :json
          [name, "hstore_merge(#{column.name}_now) OVER #{window} - hstore_merge(#{column.name}_was) OVER #{window}"]
        else
          [name, "COALESCE(#{column.name}, FIRST_VALUE(#{column.name}) OVER #{window})"]
        end
      end
      Hash[x]
    end
    method_with_last_element :consolidated_values

    def consolidated_constraints
      consolidated_columns.reject { |name, column| [:parent_uuid, :record_uuid, :start_at].include?(name) || column.null }
    end

    def aliased_rows
      rows.select { |row| row.name }
    end

    def insert_references
      references.select { |name, reference| reference.insert }
    end

    def stage_table
      self.dup.tap do |dimension|
        case type
        when :mini, :two
          dimension.type = :stage
        when :ledger
          dimension.type = :ledger_stage
        end
        dimension.columns.each do |name, column|
          column.type = :json if column.type == :key_value
        end
      end
    end

    def shared_columns(other)
      x = []
      columns.each do |_, column|
        other.columns.each do |_, other_column|
          if column.real_name == other_column.real_name && column.type == other_column.type
            x << [other_column, column]
          end
        end
      end
      x
    end

    def as_psql
      Masamune::Template.render_to_string(dimension_template, dimension: self)
    end

    def as_file(selected_columns)
      file_columns = []
      selected_columns.each do |name|
        reference_name, column_name = Column::dereference_column_name(name)
        if reference = references[reference_name]
          file_columns << reference.columns[column_name].dup.tap { |column| column.reference = reference }
        elsif columns[column_name]
          file_columns << columns[column_name]
        end
      end
      Masamune::Schema::File.new name: "#{name}_file", columns: file_columns
    end

    private

    def ledger_table_columns
      columns.values.map do |column|
        next if reserved_column_names.include?(column.name)
        if column.type == :key_value
          column_now, column_was = column.dup, column.dup
          column_now.name, column_was.name = "#{column.name}_now", "#{column.name}_was"
          [column_now, column_was]
        else
          column
        end
      end.flatten
    end

    def initialize_ledger_table!
      return unless ledger
      @ledger_table = Masamune::Schema::Dimension.new(name: name, type: :ledger, columns: ledger_table_columns, references: references.values)
      @columns[:parent_uuid] = Masamune::Schema::Column.new(name: 'parent_uuid', type: :uuid, null: true, reference: @ledger_table, parent: self)
      @columns[:record_uuid] = Masamune::Schema::Column.new(name: 'record_uuid', type: :uuid, null: true, reference: @ledger_table, parent: self)
    end

    def initialize_primary_key_column!
      case type
      when :mini
        @columns[:id] = Masamune::Schema::Column.new(name: 'id', type: :integer, primary_key: true, parent: self)
      when :two, :ledger
        @columns[:uuid] = Masamune::Schema::Column.new(name: 'uuid', type: :uuid, primary_key: true, parent: self)
      end
    end

    def initialize_foreign_key_columns!
      case type
      when :two
        foreign_key_columns.each do |column|
          @columns[column.name] = column
        end
      end
    end

    def initialize_dimension_columns!
      case type
      when :two
        @columns[:start_at] = Masamune::Schema::Column.new(name: 'start_at', type: :timestamp, default: 'TO_TIMESTAMP(0)', index: true, parent: self)
        @columns[:end_at]  = Masamune::Schema::Column.new(name: 'end_at', type: :timestamp, null: true, index: true, parent: self)
        @columns[:version] = Masamune::Schema::Column.new(name: 'version', type: :integer, default: 1, parent: self)
        @columns[:last_modified_at] = Masamune::Schema::Column.new(name: 'last_modified_at', type: :timestamp, default: 'NOW()', parent: self)
      when :ledger
        @columns[:source_kind] = Masamune::Schema::Column.new(name: 'source_kind', type: :string, null: true, parent: self)
        @columns[:source_uuid] = Masamune::Schema::Column.new(name: 'source_uuid', type: :string, null: true, parent: self)
        @columns[:start_at] = Masamune::Schema::Column.new(name: 'start_at', type: :timestamp, index: true, parent: self)
        @columns[:last_modified_at] = Masamune::Schema::Column.new(name: 'last_modified_at', type: :timestamp, default: 'NOW()', parent: self)
        @columns[:delta] = Masamune::Schema::Column.new(name: 'delta', type: :integer, parent: self)
      end
    end

    def reserved_column_names
      @reserved_column_names ||=
      case type
      when :two
        [:parent_uuid, :record_uuid, :start_at, :end_at, :version, :last_modified_at]
      when :ledger
        [:source_kind, :source_uuid, :start_at, :last_modified_at, :delta]
      else
        []
      end
    end

    def dimension_template
      @dimension_template ||= ::File.expand_path(::File.join(__FILE__, '..', 'dimension.psql.erb'))
    end
  end
end
