module Masamune::Schema
  class File
    extend Forwardable

    def_delegators :@io, :flush, :path

    DEFAULT_ATTRIBUTES =
    {
      id:      nil,
      format:  :csv,
      headers: false,
      columns: {},
      debug:   false
    }

    DEFAULT_ATTRIBUTES.keys.each do |attr|
      attr_accessor attr
    end

    def initialize(opts = {})
      opts.symbolize_keys!
      raise ArgumentError, 'required parameter id: missing' unless opts.key?(:id)
      DEFAULT_ATTRIBUTES.merge(opts).each do |name, value|
        send("#{name}=", value)
      end

      @io = ::File.open(::File::NULL, "w")
      @total_lines = 0
    end

    def columns=(instance)
      @columns  = {}
      columns = (instance.is_a?(Hash) ? instance.values : instance).compact
      columns.each do |column|
        @columns[column.name] = column
      end
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
