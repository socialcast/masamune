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
        row = Masamune::Schema::Row.new(reference: self, values: data.to_hash, strict: false)
        yield row.to_hash
      end
    end

    def append(data)
      append_with_format(columns.keys) if headers && @total_lines < 1
      append_with_format(Masamune::Schema::Row.new(reference: self, values: data))
    end

    # TODO create temporary file if buffer is not a file
    def path
      @buffer.flush
      @buffer.path
    end

    def to_s
      [path, ::File.read(path)].join("\n")
    end

    def as_table
      Masamune::Schema::Dimension.new name: name, type: :stage, columns: columns.values
    end

    private

    def append_with_format(data)
      @total_lines += 1
      buffer << data.to_csv
    end
  end
end
