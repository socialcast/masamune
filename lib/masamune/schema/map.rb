module Masamune::Schema
  class Map
    attr_accessor :name
    attr_accessor :fields
    attr_accessor :headers
    attr_accessor :debug

    def initialize(name: name, fields: {}, headers: false, debug: false)
      @name     = name
      @fields   = fields
      @debug    = debug
      @headers  = headers
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
