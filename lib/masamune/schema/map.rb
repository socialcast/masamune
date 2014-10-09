require 'csv'

module Masamune::Schema
  class Map
    class Buffer
      extend Forwardable

      def_delegators :@io, :flush, :path
      def_delegators :@map, :source, :target, :fields, :headers

      def initialize(map, io)
        @map    = map
        @io     = io
        @lines  = 0
      end

      def each(&block)
        opts = {headers: headers}
        opts.merge!(col_sep: "\t") if source.format == :tsv
        CSV.parse(@io, opts) do |data|
          row = Masamune::Schema::Row.new(parent: source, values: parse(data), strict: false)
          yield row.to_hash
        end
      end

      def append(data)
        row = Masamune::Schema::Row.new(parent: target, values: data)
        append_with_format(row.values.keys) if headers && @lines < 1
        append_with_format(row)
      end

      def append_with_format(data)
        @io ||= Tempfile.new('masamune')
        @lines += 1
        @io << encode(data)
      end

      private

      def parse(data)
        if headers
          data.to_hash
        else
          Hash[source.columns.keys.zip(data)]
        end
      end

      def encode(data)
        case source.format
        when :csv
          data.to_csv
        when :tsv
          data.to_tsv
        end
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
      input_buffer, output_buffer = Buffer.new(self, input_stream), Buffer.new(self, output_stream)
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
