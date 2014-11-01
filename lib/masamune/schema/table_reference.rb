module Masamune::Schema
  class TableReference < Delegator
    DEFAULT_ATTRIBUTES =
    {
      label:           nil,
      insert:          false,
    }

    DEFAULT_ATTRIBUTES.keys.each do |attr|
      attr_accessor attr
    end

    def initialize(table, opts = {})
      @table = table
      DEFAULT_ATTRIBUTES.merge(opts).each do |name, value|
        public_send("#{name}=", value)
      end
    end

    def __getobj__
      @table
    end

    def __setobj__(obj)
      @table = obj
    end

    def id
      [label, @table.id].compact.join('_').to_sym
    end

    def foreign_key_name
      [label, @table.name, @table.primary_key.try(:name)].compact.join('_').to_sym
    end

    def foreign_key_type
      @table.primary_key.type
    end

    def default
      @table.rows.detect { |row| row.default }.try(:name)
    end
  end
end

