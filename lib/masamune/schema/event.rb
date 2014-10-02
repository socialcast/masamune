# TODO consider database/schema for views
# TODO consider columnar store materialized view
module Masamune::Schema
  class Event
    class Attribute
      attr_accessor :id
      attr_accessor :type
      attr_accessor :immutable

      def initialize(opts = {})
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
      raise ArgumentError, 'required parameter id: missing' unless opts.key?(:id)
      DEFAULT_ATTRIBUTES.merge(opts).each do |name, value|
        send("#{name}=", value)
      end
    end

    def attributes=(attributes)
      @attributes = {}
      attributes.each do |attribute|
        @attributes[attribute.id] = attribute
      end
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
  end
end
