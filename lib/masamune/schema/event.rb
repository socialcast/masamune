module Masamune::Schema
  class Event
    class Attribute
      attr_accessor :id
      attr_accessor :type
      attr_accessor :immutable

      def initialize(opts = {})
        opts.symbolize_keys!
        raise ArgumentError, 'required parameter id: missing' unless opts.key?(:id)
        self.id = opts[:id].to_sym
        self.type = opts.fetch(:type, :integer).to_sym
        self.immutable = opts.fetch(:immutable, false)
      end
    end

    DEFAULT_ATTRIBUTES =
    {
      id:              nil,
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

    def kind
      :hql
    end

    def headers
      false
    end

    def format
      :tsv
    end

    def columns
      @columns ||= {}.tap do |columns|
        columns[:uuid] = Column.new id: :uuid, type: :uuid, parent: self
        columns[:type] = Column.new id: :type, type: :string, parent: self
        attributes.map do |_, attribute|
          if attribute.immutable
            columns[attribute.id] = Column.new id: attribute.id, type: attribute.type, parent: self
          else
            columns[:"#{attribute.id}_now"] = Column.new id: "#{attribute.id}_now", type: attribute.type, parent: self
            columns[:"#{attribute.id}_was"] = Column.new id: "#{attribute.id}_was", type: attribute.type, parent: self
          end
        end
        # TODO consider if this should be part of standard event, can derive from type
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

    def as_table(parent)
    end
  end
end
