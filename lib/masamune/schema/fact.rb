module Masamune::Schema
  class Fact
    include Masamune::LastElement

    attr_accessor :name
    attr_accessor :type
    attr_accessor :references
    attr_accessor :columns
    attr_accessor :debug

    alias measures columns

    def initialize(name: name, references: [], columns: [], debug: false)
      @name       = name
      @debug      = debug

      @references = {}
      references.each do |reference|
        @references[reference.name] = reference
      end

      @columns = {}
      initialize_foreign_key_columns!

      columns.each do |column|
        @columns[column.name] = column
        @columns[column.name].parent = self
      end
      initialize_column! name: 'last_modified_at', type: :timestamp, default: 'NOW()'
    end

    def table_name
      "#{name}_fact"
    end

    def defined_columns
      columns.values
    end
    method_with_last_element :defined_columns

    def unique_columns
      return {} if temporary?
      columns.select { |_, column| column.unique } || {}
    end

    def index_columns
      return [] if temporary?
      indices = columns.select { |_, column| column.index }.lazy
      indices = indices.group_by { |_, column| column.index == true ? column.name : column.index }.lazy
      indices = indices.map { |_, index_and_columns| index_and_columns.map(&:last) }.lazy
      indices.map do |columns|
        [columns.map(&:name), columns.all? { |column| column.unique }]
      end
    end

    def insert_rows
      []
    end

    def aliased_rows
      []
    end

    def ledger
      false
    end

    def temporary?
      false
    end

    def type
      :fact
    end

    def as_psql
      Masamune::Template.render_to_string(dimension_template, dimension: self)
    end

    private

    def initialize_foreign_key_columns!
      references.map do |_, dimension|
        initialize_column! name: dimension.primary_key.name, type: dimension.primary_key.type, reference: dimension, default: dimension.default_foreign_key_row, index: true
      end
    end

    def initialize_column!(options = {})
      column = Masamune::Schema::Column.new(options.merge(parent: self))
      @columns[column.name] = column
      @columns[column.name].parent = self
    end

    def dimension_template
      @dimension_template ||= ::File.expand_path(::File.join(__FILE__, '..', 'dimension.psql.erb'))
    end
  end
end
