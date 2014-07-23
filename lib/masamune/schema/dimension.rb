module Masamune::Schema
  class Dimension
    attr_accessor :name
    attr_accessor :columns

    def initialize(name, columns: [])
      @name     = name
      @columns  = columns
    end

    def to_psql
      Masamune::Template.render_to_string(psql_template, dimension: self)
    end

    private

    def psql_template
      @psql_template ||= File.expand_path(File.join(__FILE__, '..', 'dimension.psql.erb'))
    end
  end
end
