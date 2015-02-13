module Masamune::Schema
  class TableReference < SimpleDelegator
    DEFAULT_ATTRIBUTES =
    {
      label:           nil,
      insert:          false,
      null:            false,
      default:         nil,
      natural_key:     false,
      denormalize:     false,
      multiple:        false
    }

    DEFAULT_ATTRIBUTES.keys.each do |attr|
      attr_accessor attr
    end

    def initialize(table, opts = {})
      super table
      @table = table
      DEFAULT_ATTRIBUTES.merge(opts).each do |name, value|
        public_send("#{name}=", value)
      end
    end

    def id
      [label, @table.id].compact.join('_').to_sym
    end

    def alias
      [label, @table.name].compact.join('_')
    end

    def foreign_key
      @table.surrogate_key
    end

    def foreign_key_name
      [label, @table.name, foreign_key.name].compact.join('_').to_sym
    end

    def foreign_key_type
      foreign_key.type
    end

    def default
      return if @default == :null
      @default || @table.rows.detect { |row| row.default }.try(:name)
    end
  end
end
