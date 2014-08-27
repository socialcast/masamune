module Masamune::Schema
  class File
    extend Forwardable

    attr_accessor :id
    attr_accessor :format
    attr_accessor :headers
    attr_accessor :columns
    attr_accessor :debug

    def_delegators :@io, :flush, :path

    def initialize(opts = {})
      @id      = opts.fetch(:id, nil)
      @format  = opts.fetch(:format, :csv)
      @headers = opts.fetch(:headers, false)
      columns  = opts.fetch(:columns, {})
      @debug   = opts.fetch(:debug, false)
      @io      = ::File.open(::File::NULL, "w")

      @columns  = {}
      columns.each do |column|
        @columns[column.name] = column
      end

      @total_lines = 0
    end

    def bind(input)
      @io = input
      dup
    end

    def each(&block)
      ::CSV.parse(@io, headers: true) do |data|
        row = Masamune::Schema::Row.new(parent: self, values: data.to_hash, strict: false)
        yield row.to_hash
      end
    end

    def append(data)
      append_with_format(columns.keys) if headers && @total_lines < 1
      append_with_format(Masamune::Schema::Row.new(parent: self, values: data))
    end

    def to_s
      [path, ::File.read(path)].join("\n")
    end

    def as_table(parent = nil)
      if parent
        parent.class.new(id: id, type: :stage, columns: columns.values, parent: parent, inherit: true)
      else
        Masamune::Schema::Table.new id: id, type: :stage, columns: columns.values
      end
    end

    private

    def append_with_format(data)
      @io ||= Tempfile.new('masamune')
      @total_lines += 1
      @io << data.to_csv
    end
  end
end
