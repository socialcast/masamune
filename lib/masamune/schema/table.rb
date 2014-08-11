module Masamune::Schema
  class Table
    include Masamune::LastElement

    attr_accessor :id
    attr_accessor :type
    attr_accessor :label
    attr_accessor :references
    attr_accessor :columns
    attr_accessor :rows
    attr_accessor :insert
    attr_accessor :parent
    attr_accessor :children
    attr_accessor :debug

    def initialize(id: id, type: :table, label: nil, references: [], columns: [], rows: [], insert: false, parent: nil, debug: false)
      self.id = id
      @type   = type
      @label  = label
      @insert = insert
      @parent = parent
      @debug  = debug

      @children = []

      @references = {}
      references.each do |reference|
        @references[reference.id] = reference
      end

      columns.compact!
      raise ArgumentError, "table #{name} contains reserved columns" if columns.any? { |column| reserved_column_ids.include?(column.id) }

      @columns = {}
      initialize_primary_key_column! unless columns.any? { |column| column.primary_key }
      initialize_foreign_key_columns!
      columns.each do |column|
        column.parent = self
        @columns[column.name.to_sym] = column
      end

      @rows = []
      rows.each do |row|
        row.parent = self
        @rows << row
      end
    end

    def initialize_copy(other)
      other.columns.each do |_, other_column|
        next unless other_column
        column = other_column.dup
        column.parent = self
        @columns[column.name.to_sym] = column
      end

      other.rows.each do |_, other_row|
        next unless other_row
        row = other_row.dup
        row.parent = self
        @rows << row
      end
    end

    def id=(id)
      @id = id.to_sym
    end

    def name
      case type
      when :stage
        parent ? "#{parent.name}_stage" : "#{@id}_stage"
      when :table
        "#{@id}_table"
      end
    end

    def temporary?
      type == :stage
    end

    def primary_key
      columns.values.detect { |column| column.primary_key }
    end

    def surrogate_keys
      columns.values.select { |column| column.surrogate_key }
    end

    def foreign_key_name
      [label, name, primary_key.try(:name)].compact.join('_')
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
      columns.values.reject { |column| reserved_column_ids.include?(column.id) || column.primary_key || column.surrogate_key }
    end
    method_with_last_element :upsert_update_columns

    def upsert_insert_columns
      columns.values.reject { |column| column.primary_key }
    end
    method_with_last_element :upsert_insert_columns

    def foreign_key_columns
      columns.values.select { | column| column.reference }
    end

    def upsert_unique_columns
      columns.values.select { |column| column.surrogate_key }
    end

    def default_foreign_key_name
      rows.detect { |row| row.default }.try(:name)
    end

    def insert_rows
      rows.select { |row| row.insert_values.any? }
    end

    def insert_values
      columns.map do |_, column|
        if reference = column.reference
          select = "(SELECT #{reference.primary_key.name} FROM #{reference.name} WHERE #{column.foreign_key_name} = #{column.name})"
          if reference.default_foreign_key_name
            "COALESCE(#{select}, #{reference.default_foreign_key_name})"
          else
            select
          end
        elsif column.type == :key_value
          "json_to_hstore(#{column.name})"
        else
          column.name.to_s
        end
      end
    end
    method_with_last_element :insert_values

    def aliased_rows
      rows.select { |row| row.name }
    end

    def insert_references
      references.select { |_, reference| reference.insert }
    end

    def stage_table
      @stage_table ||= self.dup.tap do |table|
        table.parent = self
        table.type = :stage
        table.columns.each do |_, column|
          column.strict = false
        end
      end
    end

    def shared_columns(other)
      [].tap do |shared|
        columns.each do |_, column|
          other.columns.each do |_, other_column|
            next unless column.id == other_column.id
            next unless column.type == other_column.type
            shared << [other_column, column]
          end
        end
      end
    end

    def as_psql
      Masamune::Template.render_to_string(table_template, table: self)
    end

    def as_file(a = [])
      Masamune::Schema::File.new id: id, columns: select_columns(a)
    end

    private

    def initialize_primary_key_column!
      case type
      when :table
        initialize_column! id: 'uuid', type: :uuid, primary_key: true
      end
    end

    def initialize_foreign_key_columns!
      references.map do |_, table|
        initialize_column! id: table.foreign_key_name, type: table.primary_key.type, reference: table, default: table.default_foreign_key_name
      end
    end

    def initialize_column!(options = {})
      column = Masamune::Schema::Column.new(options.merge(parent: self))
      @columns[column.name.to_sym] = column
    end

    def select_columns(a = [])
      [].tap do |result|
        a.each do |name|
          reference_name, column_name = Column::dereference_column_name(name)
          if reference = references[reference_name]
            result << reference.columns[column_name].dup.tap { |column| column.reference = reference }
          elsif columns[column_name]
            result << columns[column_name]
          end
        end
      end
    end

    def reserved_column_ids
      @reserved_column_ids ||= []
    end

    def table_template
      @table_template ||= ::File.expand_path(::File.join(__FILE__, '..', 'table.psql.erb'))
    end
  end
end
