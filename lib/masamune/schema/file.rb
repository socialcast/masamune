module Masamune::Schema
  class File
    attr_accessor :name
    attr_accessor :format
    attr_accessor :headers
    attr_accessor :columns
    attr_accessor :debug

    def initialize(name: nil, format: :csv, headers: false, columns: {}, debug: false)
      @name    = name
      @format  = format
      @headers = headers
      @debug   = debug
      @io      = nil

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

    def bind(input)
      @io = input
      dup
    end

    def each(&block)
      ::CSV.parse(@io, headers: true) do |data|
        row = Masamune::Schema::Row.new(reference: self, values: data.to_hash, strict: false)
        yield row.to_hash
      end
    end

    def append(data)
      append_with_format(columns.keys) if headers && @total_lines < 1
      append_with_format(Masamune::Schema::Row.new(reference: self, values: data))
    end

    def flush
      @io.flush
    end

    def path
      @io.path
    end

    def to_s
      [path, ::File.read(path)].join("\n")
    end

    def as_table
      Masamune::Schema::Dimension.new name: name, type: :stage, columns: columns.values
    end

    private

    def append_with_format(data)
      @io ||= Tempfile.new('masamune')
      @total_lines += 1
      @io << data.to_csv
    end
  end
end
