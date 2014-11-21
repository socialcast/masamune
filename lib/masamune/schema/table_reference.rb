module Masamune::Schema
  class TableReference < Delegator
    DEFAULT_ATTRIBUTES =
    {
      label:           nil,
      insert:          false,
      null:            false,
      default:         nil,
      natural_key:     false
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

    def alias
      [label, @table.name].compact.join('_')
    end

    def foreign_key_name
      [label, @table.name, @table.surrogate_key.try(:name)].compact.join('_').to_sym
    end

    def foreign_key_type
      @table.surrogate_key.type
    end

    def default
      return if @default == :null
      @default || @table.rows.detect { |row| row.default }.try(:name)
    end
  end
end
