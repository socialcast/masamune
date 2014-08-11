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

    def as_psql
      Masamune::Template.combine \
        stage_fact_as_psql
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
  end
end
