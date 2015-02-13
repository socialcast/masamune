module Masamune::Schema
  class Event
    class Attribute
      attr_accessor :id
      attr_accessor :type
      attr_accessor :array
      attr_accessor :immutable

      def initialize(opts = {})
        opts.symbolize_keys!
        raise ArgumentError, 'required parameter id: missing' unless opts.key?(:id)
        self.id = opts[:id].to_sym
        self.type = opts.fetch(:type, :integer).to_sym
        self.array = opts.fetch(:array, false)
        self.immutable = opts.fetch(:immutable, false)
      end

      def as_columns(event, &block)
        column_ids = immutable ? [id] : [:"#{id}_now", :"#{id}_was"]
        column_ids.each do |id|
          yield [id, Column.new(id: id, type: type, array: array, parent: event)]
        end
      end
    end

    DEFAULT_ATTRIBUTES =
    {
      id:              nil,
      store:           nil,
      attributes:      [],
      debug:           false
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

    def attributes=(attributes)
      @attributes = {}
      attributes.each do |attribute|
        @attributes[attribute.id] = attribute
      end
    end

    def columns
      @columns ||= {}.tap do |columns|
        columns[:uuid] = Column.new id: :uuid, type: :uuid, parent: self
        columns[:type] = Column.new id: :type, type: :string, parent: self
        attributes.map do |_, attribute|
          attribute.as_columns(self) do |id, column|
            columns[id] = column
          end
        end
        columns[:delta] = Column.new id: :delta, type: :integer, parent: self
        columns[:created_at] = Column.new id: :created_at, type: :timestamp, parent: self
      end
    end

    def reserved_column_ids
      @reserved_column_ids ||= [:uuid, :type, :delta, :created_at]
    end

    def unreserved_columns
      columns.reject { |_, column| reserved_column_ids.include?(column.id) }
    end

    def create_type
      @create_type ||= "#{id}_create"
    end

    def update_type
      @update_type ||= "#{id}_update"
    end

    def delete_type
      @delete_type ||= "#{id}_delete"
    end

    def dereference_column_name(name)
      columns[name.to_sym]
    end
  end
end
