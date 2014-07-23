module Masamune::Schema
  class Dimension
    attr_accessor :name
    attr_accessor :type
    attr_accessor :references
    attr_accessor :columns
    attr_accessor :values

    def initialize(name: name, type: :two, references: [], columns: [], values: [])
      @name       = name.to_sym
      @type       = type
      @references = references
      @columns    = columns
      @values     = values

      initialize_default_columns_for_type!
    end

    def table_name
      case type
      when :mini
        "#{name}_type"
      when :two
        "#{name}_dimension"
      end
    end

    def primary_key
      columns.select { |column| column.primary_key }.first
    end

    def index_columns
      columns.select { |column| column.index }
    end

    def unique_columns
      columns.select { |column| column.unique }
    end

    def foreign_key_columns
      references.map do |dimension|
        name = "#{dimension.table_name}_#{dimension.primary_key.name}"
        type = dimension.primary_key.type
        Masamune::Schema::Column.new(name: name, type: type, reference: dimension)
      end
    end

    def to_s
      Masamune::Template.render_to_string(dimension_template, dimension: self)
    end

    private

    def initialize_default_columns_for_type!
      case type
      when :two
        @columns.unshift Masamune::Schema::Column.new(name: 'uuid', type: :uuid, primary_key: true)
        @columns += foreign_key_columns if foreign_key_columns
        @columns.append Masamune::Schema::Column.new(name: 'start_at', type: :timestamp, default: 'TO_TIMESTAMP(0)', index: true)
        @columns.append Masamune::Schema::Column.new(name: 'end_at', type: :timestamp, null: true, index: true)
        @columns.append Masamune::Schema::Column.new(name: 'version', type: :integer, default: 1)
        @columns.append Masamune::Schema::Column.new(name: 'last_modified_at', type: :timestamp, default: 'NOW()')
      end
    end

    def dimension_template
      @dimension_template ||= File.expand_path(File.join(__FILE__, '..', 'dimension.psql.erb'))
    end
  end
end
