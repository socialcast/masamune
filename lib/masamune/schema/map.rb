module Masamune::Schema
  class Map
    attr_accessor :fields
    attr_accessor :debug
    attr_accessor :headers

    def initialize(fields: {}, debug: false, headers: false)
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
        fields.each do |field, function|
          case function
          when String, Symbol
            output[field] = input[function]
          when Proc
            output[field] = function.call(input)
          else
            output[field] = function
          end
        end
        target.append output
      end
      target
    end
  end
end
