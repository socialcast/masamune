# TODO prevent reserved_column_name collision
module Masamune::Schema
  class Dimension
    attr_accessor :name
    attr_accessor :type
    attr_accessor :ledger
    attr_accessor :references
    attr_accessor :columns
    attr_accessor :rows
    attr_accessor :ledger_table

    def initialize(name: name, type: :two, ledger: false, references: [], columns: [], rows: [])
      @name       = name.to_sym
      @type       = type
      @ledger     = ledger
      @rows       = rows

      @references = {}
      references.each do |reference|
        @references[reference.name] = reference
      end

      @columns = {}
      initialize_primary_key_column! unless columns.any? { |column| column.primary_key }
      initialize_foreign_key_columns!
      columns.each do |column|
        @columns[column.name] = column
      end
      initialize_ledger_table!
      initialize_dimension_columns!

      @rows.each { |row| row.dimension = self }
    end

    def table_name
      case type
      when :mini
        "#{name}_type"
      when :two
        "#{name}_dimension"
      when :ledger
        "#{name}_dimension_ledger"
      end
    end

    def primary_key
      columns.each do |_, column|
        return column if column.primary_key
      end
    end

    def index_columns
      columns.select { |_, column| column.index }
    end

    def unique_columns
      columns.select { |_, column| column.unique }
    end

    def foreign_key_columns
      references.map do |_, dimension|
        name = "#{dimension.table_name}_#{dimension.primary_key.name}"
        type = dimension.primary_key.type
        Masamune::Schema::Column.new(name: name, type: type, reference: dimension, default: dimension.default_foreign_key_row)
      end
    end

    def default_foreign_key_row
      rows.select { |row| row.default }.first.try(:name)
    end

    def insert_rows
      rows.select { |row| row.insert_values.any? }
    end

    def aliased_rows
      rows.select { |row| row.name }
    end

    def to_s
      Masamune::Template.render_to_string(dimension_template, dimension: self)
    end

    def defined_columns
      columns.values.reject { |column| reserved_column_names.include?(column.name) }
    end

    private

    def initialize_ledger_table!
      return unless ledger
      @ledger_table = Masamune::Schema::Dimension.new(name: name, type: :ledger, columns: defined_columns)
      @columns[:parent_uuid] = Masamune::Schema::Column.new(name: 'parent_uuid', type: :uuid, null: true, reference: @ledger_table)
      @columns[:record_uuid] = Masamune::Schema::Column.new(name: 'record_uuid', type: :uuid, null: true, reference: @ledger_table)
    end

    def initialize_primary_key_column!
      case type
      when :mini
        @columns[:id] = Masamune::Schema::Column.new(name: 'id', type: :integer, primary_key: true)
      when :two, :ledger
        @columns[:uuid] = Masamune::Schema::Column.new(name: 'uuid', type: :uuid, primary_key: true)
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
        @columns[:start_at] = Masamune::Schema::Column.new(name: 'start_at', type: :timestamp, default: 'TO_TIMESTAMP(0)', index: true)
        @columns[:end_at]  = Masamune::Schema::Column.new(name: 'end_at', type: :timestamp, null: true, index: true)
        @columns[:version] = Masamune::Schema::Column.new(name: 'version', type: :integer, default: 1)
        @columns[:last_modified_at] = Masamune::Schema::Column.new(name: 'last_modified_at', type: :timestamp, default: 'NOW()')
      when :ledger
        @columns[:source_kind] = Masamune::Schema::Column.new(name: 'source_kind', type: :string, null: true)
        @columns[:source_uuid] = Masamune::Schema::Column.new(name: 'source_uuid', type: :string, null: true)
        @columns[:start_at] = Masamune::Schema::Column.new(name: 'start_at', type: :timestamp, index: true)
        @columns[:last_modified_at] = Masamune::Schema::Column.new(name: 'last_modified_at', type: :timestamp, default: 'NOW()')
        @columns[:delta] = Masamune::Schema::Column.new(name: 'delta', type: :integer)
      end
    end

    def reserved_column_names
      @reserved_column_names ||=
      case type
      when :two
        [:parent_uuid, :record_uuid, :start_at, :end_at, :version, :last_modified_at]
      when :ledger
        [:source_kind, :source_uuid, :start_at, :last_modified_at, :delta]
      end
    end

    def dimension_template
      @dimension_template ||= File.expand_path(File.join(__FILE__, '..', 'dimension.psql.erb'))
    end
  end
end
