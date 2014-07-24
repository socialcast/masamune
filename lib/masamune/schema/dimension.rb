# TODO check for reserved column names
module Masamune::Schema
  class Dimension
    attr_accessor :name
    attr_accessor :type
    attr_accessor :references
    attr_accessor :functions
    attr_accessor :columns
    attr_accessor :values

    def initialize(name: name, type: :two, references: [], columns: [], values: [])
      @name       = name.to_sym
      @type       = type
      @values     = values

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
      initialize_dimension_columns!
      validate_values!

      @functions = {}
      initialize_default_foreign_key_functions!
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

    def default_record?
      columns.any? { |_, column| column.name == :default_record }
    end

    def foreign_key_columns
      references.map do |_, dimension|
        name = "#{dimension.table_name}_#{dimension.primary_key.name}"
        type = dimension.primary_key.type
        Masamune::Schema::Column.new(name: name, type: type, reference: dimension, default: dimension.default_foreign_key_record)
      end
    end

    def default_foreign_key_record
      return unless default_record?
      "default_#{table_name}_#{primary_key.name}()"
    end

    def insert_values(&block)
      values.each do |record|
        unique_values = record.slice(*unique_columns.keys)
        next if unique_values.empty?
        unique_constraints = unique_values.map { |key, value| "#{key} = #{columns[key].sql_value(value)}" }.compact
        insert_values = record.map { |key, value| columns[key].sql_value(value) }
        yield record.keys, insert_values, unique_constraints
      end
    end

    def to_s
      Masamune::Template.render_to_string(dimension_template, dimension: self)
    end

    private

    def initialize_primary_key_column!
      case type
      when :mini
        @columns[:id] = Masamune::Schema::Column.new(name: 'id', type: :integer, primary_key: true)
      when :two
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
      @columns[:default_record] = Masamune::Schema::Column.new(name: 'default_record', type: :boolean, default: false) if values.any? { |record| record.key?(:default_record) }

      case type
      when :two
        @columns[:start_at] = Masamune::Schema::Column.new(name: 'start_at', type: :timestamp, default: 'TO_TIMESTAMP(0)', index: true)
        @columns[:end_at]  = Masamune::Schema::Column.new(name: 'end_at', type: :timestamp, null: true, index: true)
        @columns[:version] = Masamune::Schema::Column.new(name: 'version', type: :integer, default: 1)
        @columns[:last_modified_at] = Masamune::Schema::Column.new(name: 'last_modified_at', type: :timestamp, default: 'NOW()')
      end
    end

    def initialize_default_foreign_key_functions!
      return unless default_foreign_key_record
      @functions[default_foreign_key_record] = <<-EOS.strip_heredoc
      CREATE OR REPLACE FUNCTION #{default_foreign_key_record}
      RETURNS #{primary_key.sql_type} IMMUTABLE AS $$
        SELECT #{primary_key.name} FROM #{table_name} WHERE default_record = TRUE;
      $$ LANGUAGE SQL;
      EOS
    end

    def validate_values!
      values.each do |record|
        undefined_columns = record.keys - columns.keys
        raise ArgumentError, "#{record} contains undefined columns #{undefined_columns}" if undefined_columns.any?
      end
    end

    def dimension_template
      @dimension_template ||= File.expand_path(File.join(__FILE__, '..', 'dimension.psql.erb'))
    end
  end
end
