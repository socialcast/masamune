module Masamune::Schema
  class Map
    attr_accessor :id
    attr_accessor :fields
    attr_accessor :headers
    attr_accessor :debug

    def initialize(opts = {})
      @id      = opts.fetch(:id, nil)
      @fields  = opts.fetch(:fields, {})
      @headers = opts.fetch(:headers, false)
      @debug   = opts.fetch(:debug, false)
    end

    def columns
      @fields.symbolize_keys.keys
    end

    def apply(source, target)
      target.headers = headers
      source.each do |input|
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
        target.append output
      end
      target.flush
      target
    end
  end
end
