module Masamune::Transform
  class LoadDimension
    def initialize(source, target, map)
      @source = source
      @target = target.ledger ? target.ledger_table : target
      @map    = map
    end

    def as_psql
      tmp = @target.as_file(@map.columns)
      tmp.buffer = Tempfile.new('masamune')
      @map.apply(@source, tmp)
      Masamune::Template.render_to_string(load_dimension_template, source: tmp.as_table, source_file: tmp.path, target: @target)
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
