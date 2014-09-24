module Masamune::Schema
  class Event
    DEFAULT_ATTRIBUTES =
    {
      id:              nil,
      columns:         {},
      debug:           false
    }

    DEFAULT_ATTRIBUTES.keys.each do |attr|
      attr_accessor attr.to_sym
    end

    alias_method :attributes, :columns
    alias_method :attributes=, :columns=

    def initialize(opts = {})
      # TODO raise ArgumentError unless id
      # TODO Use opts everywhere
      DEFAULT_ATTRIBUTES.merge(opts).each do |name, value|
        send("#{name}=", value)
      end
    end

    def columns=(instance)
      @columns = {}
      columns = (instance.is_a?(Hash) ? instance.values : instance).compact

      columns.each do |column|
        column.parent = self
        @columns[column.name.to_sym] = column
      end
    end
  end
end
