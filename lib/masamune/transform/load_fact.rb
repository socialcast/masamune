module Masamune::Transform
  class LoadFact
    def initialize(source_files, source, target)
      @source_files = source_files
      @source = source
      @target = target
    end

    def stage_fact_as_psql
      Masamune::Template.render_to_string(stage_fact_template, source: @source, source_files: @source_files)
    end

    def insert_reference_values_as_psql
      Masamune::Template.render_to_string(insert_reference_values_template, source: @source, target: @target)
    end

    def load_fact_as_psql
      Masamune::Template.render_to_string(load_fact_template, source: @source, target: @target)
    end

    def as_psql
      Masamune::Template.combine \
        stage_fact_as_psql,
        insert_reference_values_as_psql
    end

    def to_psql_file
      Tempfile.new('masamune').tap do |file|
        file.write(as_psql)
        file.close
      end.path
    end

    private

    def stage_fact_template
      @stage_fact_template ||= File.expand_path(File.join(__FILE__, '..', 'stage_fact.psql.erb'))
    end

    def insert_reference_values_template
      @insert_reference_values_template ||= File.expand_path(File.join(__FILE__, '..', 'insert_reference_values.psql.erb'))
    end

    def load_fact_template
      @load_fact_template ||= File.expand_path(File.join(__FILE__, '..', 'load_fact.psql.erb'))
    end
  end
end
