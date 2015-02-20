require 'csv'

module Masamune::Schema
  class Map
    class Buffer
      extend Forwardable

      def_delegators :@store, :headers, :format
      def_delegators :@io, :flush, :path

      def initialize(table, io)
        @table  = table
        @store  = table.store
        @io     = io.set_encoding('binary', 'UTF-8', undef: :replace)
      end

      def each(&block)
        CSV.parse(@io, options.merge(headers: @store.headers || @table.columns.keys)) do |data|
          row = Masamune::Schema::Row.new(parent: @table, values: data.to_hash, strict: false)
          yield row.to_hash
        end
      end

      def append(data)
        row = Masamune::Schema::Row.new(parent: @table, values: data.to_hash)
        @io ||= Tempfile.new('masamune')
        @csv ||= CSV.new(@io, options.merge(headers: row.headers, write_headers: @store.headers))
        @csv << row.serialize
      end

      def options
        if @store.format == :tsv
          { col_sep: "\t" }
        else
          {}
        end
      end
    end

    DEFAULT_ATTRIBUTES =
    {
      source:    nil,
      target:    nil,
      store:     nil,
      function:  ->(row) { row },
      debug:     false
    }

    DEFAULT_ATTRIBUTES.keys.each do |attr|
      attr_accessor attr
    end

    def initialize(opts = {})
      opts.symbolize_keys!
      raise ArgumentError, 'required parameter source: missing' unless opts.key?(:source)
      raise ArgumentError, 'required parameter target: missing' unless opts.key?(:target)
      DEFAULT_ATTRIBUTES.merge(opts).each do |name, value|
        public_send("#{name}=", value)
      end
    end

    def source=(source)
      @source = source
    end

    # FIXME: avoid implict conversions
    def target=(target)
      @target = target.type == :four ? target.ledger_table : target
    end

    def columns
      function.call({}).keys
    end

    def intermediate
      target.stage_table(columns: columns, inherit: false)
    end

    def apply(input_files, output_file)
      Array.wrap(input_files).each do |input_file|
        File.open(input_file, 'r') do |input_stream|
          File.open(output_file, 'a+') do |output_stream|
            apply_stream(input_stream, output_stream)
          end
        end
      end
      intermediate
    end

    private

    def apply_stream(input_stream, output_stream)
      input_buffer = Buffer.new(source, input_stream)
      output_buffer = Buffer.new(intermediate, output_stream)
      input_buffer.each do |input|
        Array.wrap(function.call(input)).each do |output|
          output_buffer.append output
        end
      end
      output_buffer.flush
    end
  end
end
