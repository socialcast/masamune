module Masamune::Transform
  class LoadDimension
    attr_accessor :output

    def initialize(file, source, target, map)
      @source = source.bind(file)
      @target = target.type == :four ? target.ledger_table : target
      @map    = map
    end

    def run
      @output = @target.as_file(@map.columns)
      @map.apply(@source, @output.bind(Tempfile.new('masamune')))
    end

    def stage_dimension_as_psql
      # FIXME move map.apply out of transformation
      FileUtils.chmod(0777 - File.umask, output.path) if File.exists?(output.path)
      Masamune::Template.render_to_string(stage_dimension_template, source: output.as_table, source_file: output.path)
    end

    def insert_reference_values_as_psql
      Masamune::Template.render_to_string(insert_reference_values_template, source: output.as_table, target: @target)
    end

    def load_dimension_as_psql
      Masamune::Template.render_to_string(load_dimension_template, source: output.as_table, target: @target)
    end

    def as_psql
      Masamune::Template.combine \
        stage_dimension_as_psql,
        insert_reference_values_as_psql,
        load_dimension_as_psql
    end

    def to_psql_file
      Tempfile.new('masamune').tap do |file|
        file.write(as_psql)
        file.close
      end.path
    end

    private

    def stage_dimension_template
      @stage_dimension_template ||= File.expand_path(File.join(__FILE__, '..', 'stage_dimension.psql.erb'))
    end

    def insert_reference_values_template
      @insert_reference_values_template ||= File.expand_path(File.join(__FILE__, '..', 'insert_reference_values.psql.erb'))
    end

    def load_dimension_template
      @load_dimension_template ||= File.expand_path(File.join(__FILE__, '..', 'load_dimension.psql.erb'))
    end
  end
end
