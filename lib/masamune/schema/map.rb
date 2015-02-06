require 'csv'

module Masamune::Schema
  class Map
    class Buffer
      extend Forwardable

      def_delegators :@store, :headers, :format
      def_delegators :@io, :flush, :path

      def initialize(store, io)
        @store  = store
        @io     = io.set_encoding('binary', 'UTF-8', undef: :replace)
      end

      def each(&block)
        CSV.parse(@io, options.merge(headers: @store.headers || @store.columns.keys)) do |data|
          row = Masamune::Schema::Row.new(parent: @store, values: data.to_hash, strict: false)
          yield row.to_hash
        end
      end

      def append(data)
        row = Masamune::Schema::Row.new(parent: @store, values: data.to_hash)
        @io ||= Tempfile.new('masamune')
        @csv ||= CSV.new(@io, options.merge(headers: row.headers, write_headers: headers))
        @csv << row.serialize
      end

      def options
        if format == :tsv
          { col_sep: "\t" }
        else
          {}
        end
      end
    end

    DEFAULT_ATTRIBUTES =
    {
      source:  nil,
      target:  nil,
      store:   nil,
      fields:  {},
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
      input_buffer, output_buffer = Buffer.new(source, input_stream), Buffer.new(target, output_stream)
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
