# TODO CSV transform should return a table object for loading
module Masamune::Schema
  class CSVFile
    include Masamune::HasEnvironment
    include Masamune::LastElement

    attr_accessor :name
    attr_accessor :file
    attr_accessor :columns
    attr_accessor :constants

    def initialize(environment, name: nil, file: nil, columns: {}, constants: {})
      self.environment = environment

      @name = name
      @file = file

      @columns  = {}
      columns.each do |column|
        column.transform ||= ->(row) { row[column.name.to_s] }
        @columns[column.name] = column
      end

      @constants = constants
    end

    def table_name
      "#{name}_stage"
    end

    def defined_columns
      columns.values
    end
    method_with_last_element :defined_columns

    def insert_columns(dimension)
      columns.map do |_, column|
        if reference = column.reference
          reference.foreign_key_name
        else
          column.name
        end
      end + constants.keys
    end

    def insert_values(dimension)
      columns.map do |_, column|
        if reference = column.reference
          "(SELECT #{reference.primary_key.name} FROM #{reference.table_name} WHERE #{column.foreign_key_name} = #{column.name})"
        else
          column.name.to_s
        end
      end + constants.map { |key, value| dimension.columns[key].sql_value(value) }
    end
    method_with_last_element :insert_values

    def transform
      Tempfile.new('masamune').tap do |tmp|
        io = filesystem.cat(file)
        ::CSV.parse(io, headers: true) do |row|
          output = []
          columns.each do |_, column|
            output << column.transform.call(row.to_hash.with_indifferent_access)
          end
          tmp.puts(output.to_csv)
        end
        tmp.close
      end
    end

    def path
      transform.path
    end

    def as_table
      output = transform
      Masamune::Template.render_to_string(stage_template, stage: self, output: output)
    end

    private

    def stage_template
      @stage_template ||= File.expand_path(File.join(__FILE__, '..', 'stage.psql.erb'))
    end
  end
end
