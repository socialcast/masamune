module Masamune::Transform
  class LoadDimension
    def initialize(source_file, source, target)
      @source_file = source_file
      @source = source
      @target = target
    end

    def stage_dimension_as_psql
      Masamune::Template.render_to_string(stage_dimension_template, source: @source, source_file: @source_file)
    end

    def insert_reference_values_as_psql
      Masamune::Template.render_to_string(insert_reference_values_template, source: @source, target: @target)
    end

    def load_dimension_as_psql
      Masamune::Template.render_to_string(load_dimension_template, source: @source, target: @target)
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

    # TOOD move consolidate functions into transform

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
