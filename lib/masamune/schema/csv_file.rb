module Masamune::Schema
  class CSVFile
    include Masamune::HasEnvironment

    attr_accessor :name
    attr_accessor :files
    attr_accessor :columns

    def initialize(environment, name: nil, files: [], columns: [])
      self.environment = environment

      @name     = name
      @files    = files
      @columns  = columns
      @columns.each do |column|
        column.transform ||= ->(row) { row[column.name.to_s] }
      end
    end

    def table_name
      "#{name}_stage"
    end

    def transform
      Tempfile.new('masamune').tap do |tmp|
        files.map { |file| filesystem.glob(file) }.flatten.each do |file|
          io = filesystem.cat(file)
          ::CSV.parse(io, headers: true) do |row|
            output = []
            columns.each do |column|
              output << column.transform.call(row.to_hash.with_indifferent_access)
            end
            tmp.puts(output.to_csv)
          end
        end
        tmp.close
      end
    end
  end
end
