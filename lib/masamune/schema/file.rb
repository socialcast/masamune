module Masamune::Schema
  class File
    attr_accessor :name
    attr_accessor :format
    attr_accessor :headers
    attr_accessor :file
    attr_accessor :buffer
    attr_accessor :columns
    attr_accessor :debug

    def initialize(name: nil, format: :csv, headers: false, file: nil, buffer: nil, columns: {}, debug: false)
      @name    = name
      @format  = format
      @headers = headers
      @file    = file
      @buffer  = buffer
      @debug   = debug

      @columns  = {}
      columns.each do |column|
        @columns[column.name] = column
      end

      @total_lines = 0
    end

    def compact_column_names
      columns.values.map do |column|
        column.compact_name
      end
    end

    def buffer
      return @buffer if @buffer
      @buffer ||= ::File.open(file) if file
      @buffer ||= StringIO.new
    end

    def each(&block)
      ::CSV.parse(buffer, headers: true) do |data|
        row = Masamune::Schema::Row.new(values: data.to_hash, strict: false)
        row.dimension = self
        yield row.to_hash
      end
    end

    def append(data)
      if headers && @total_lines < 1
        formatted_headers = columns.keys.to_csv
        @total_lines += 1
        buffer << formatted_headers
      end

      row = Masamune::Schema::Row.new(values: data)
      row.dimension = self
      formatted_row = row.to_csv
      @total_lines += 1
      buffer << formatted_row
    end

    # TODO create temporary file if buffer is not a file
    def path
      @buffer.flush
      @buffer.path
    end

    def as_table
      Masamune::Schema::Dimension.new name: name, type: :stage, columns: columns.values
    end
  end
end
