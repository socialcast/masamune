module Masamune::Schema
  class File
    DEFAULT_ATTRIBUTES =
    {
      id:      nil,
      format:  :csv,
      headers: false,
      columns: {},
      debug:   false
    }

    DEFAULT_ATTRIBUTES.keys.each do |attr|
      attr_accessor attr
    end

    def initialize(opts = {})
      opts.symbolize_keys!
      raise ArgumentError, 'required parameter id: missing' unless opts.key?(:id)
      DEFAULT_ATTRIBUTES.merge(opts).each do |name, value|
        public_send("#{name}=", value)
      end
    end

    def type
      :file
    end

    def kind
      :file
    end

    def columns=(instance)
      @columns  = {}
      columns = (instance.is_a?(Hash) ? instance.values : instance).compact
      columns.each do |column|
        column.parent = self
        @columns[column.name] = column
      end
    end

    def as_table(table)
      table.class.new(id: id, type: :file, columns: columns.values, parent: table, inherit: true)
    end

    def as_file(columns = [])
      self
    end
  end
end
