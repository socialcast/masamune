module Masamune::Schema
  class Column
    attr_accessor :name
    attr_accessor :type
    attr_accessor :null
    attr_accessor :default

    def initialize(name, type: :integer, null: false, default: nil)
      @name     = name
      @type     = type
      @null     = null
      @default  = default
    end

    def to_psql
      "#{name} #{postgres_type} #{'NOT NULL' unless null}".strip
    end

    def postgres_type
      case type
      when :integer
        'INTEGER'
      end
    end
  end
end
