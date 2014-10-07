module Masamune::Schema
  class Map
    class CSVBuffer
      extend Forwardable

      def_delegators :@io, :flush, :path

      def initialize(table, io, options = {})
        @table = table
        @io = io
        @headers = options.fetch(:headers, false)
        @fields = options.fetch(:fields, [])
        @total_lines = 0
      end

      def each(&block)
        ::CSV.parse(@io, headers: @headers) do |data|
          row = Masamune::Schema::Row.new(parent: @table, values: data.to_hash, strict: false)
          yield row.to_hash
        end
      end

      def append(data)
        row = Masamune::Schema::Row.new(parent: @table, values: data)
        append_with_format(row.values.keys) if @headers && @total_lines < 1
        append_with_format(row)
      end

      def append_with_format(data)
        @io ||= Tempfile.new('masamune')
        @total_lines += 1
        @io << data.to_csv
      end
    end

    DEFAULT_ATTRIBUTES =
    {
      source:  nil,
      target:  nil,
      fields:  {},
      headers: false,
      debug:   false
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

    def target=(target)
      @target = target.type == :four ? target.ledger_table : target
    end

    def columns
      @fields.symbolize_keys.keys
    end

    def apply(input_stream, output_stream)
      input_buffer, output_buffer = CSVBuffer.new(@source, input_stream, headers: @source.headers), CSVBuffer.new(@target, output_stream, headers: headers, fields: @fields)
      input_buffer.each do |input|
        output = {}
        fields.each do |field, value|
          case value
          when String, Symbol
            if input.key?(value)
              output[field] = input[value]
            else
              output[field] = value
            end
          when Proc
            output[field] = value.call(input)
          else
            output[field] = value
          end
        end
        output_buffer.append output
      end
      output_buffer.flush
      @target.as_file(columns)
    end
  end
end
