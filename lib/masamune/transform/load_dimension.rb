module Masamune::Transform
  class LoadDimension
    attr_accessor :output

    def initialize(file, source, target, map)
      @source = source.bind(file)
      @target = target.ledger ? target.ledger_table : target
      @map    = map
    end

    def run
      @output = @target.as_file(@map.columns)
      @map.apply(@source, @output.bind(Tempfile.new('masamune')))
    end

    def as_psql
      Masamune::Template.render_to_string(load_dimension_template, source: @output.as_table, source_file: @output.path, target: @target)
    end

    def to_psql_file
      Tempfile.new('masamune').tap do |file|
        file.write(as_psql)
        file.close
      end.path
    end

    private

    def load_dimension_template
      @load_dimension_template ||= File.expand_path(File.join(__FILE__, '..', 'load_dimension.psql.erb'))
    end
  end
end
