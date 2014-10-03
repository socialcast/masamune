module Masamune::Schema
  class Map
    DEFAULT_ATTRIBUTES =
    {
      from:    nil,
      to:      nil,
      fields:  {},
      headers: false,
      debug:   false
    }

    DEFAULT_ATTRIBUTES.keys.each do |attr|
      attr_accessor attr
    end

    def initialize(opts = {})
      opts.symbolize_keys!
      raise ArgumentError, 'required parameter from: missing' unless opts.key?(:from)
      raise ArgumentError, 'required parameter to: missing' unless opts.key?(:to)
      DEFAULT_ATTRIBUTES.merge(opts).each do |name, value|
        public_send("#{name}=", value)
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
