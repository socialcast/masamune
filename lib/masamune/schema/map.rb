module Masamune::Schema
  class Map
    attr_accessor :id
    attr_accessor :fields
    attr_accessor :headers
    attr_accessor :debug

    DEFAULT_ATTRIBUTES =
    {
      fields: {},
      headers: false,
      debug:   false
    }

    def initialize(opts = {})
      DEFAULT_ATTRIBUTES.merge(opts).each do |name, value|
        send("#{name}=", value)
      end
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
