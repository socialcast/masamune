module Masamune::Transform
  class ConsolidateDimension
    def initialize(target)
      @target = target
      @source = target.stage_table
    end

    def bulk_upsert_as_psql
      Masamune::Template.render_to_string(bulk_upsert_template, target: @target, source: @source)
    end

    def consolidate_dimension_as_psql
      Masamune::Template.render_to_string(consolidate_dimension_template, target: @target, source: @source)
    end

    def relabel_dimension_as_psql
      Masamune::Template.render_to_string(relabel_dimension_template, target: @target)
    end

    def as_psql
      [
        consolidate_dimension_as_psql,
        bulk_upsert_as_psql,
        relabel_dimension_as_psql
      ].join("\n")
    end

    def to_psql_file
      Tempfile.new('masamune').tap do |file|
        file.write(as_psql)
        file.close
      end.path
    end

    private

    def bulk_upsert_template
      @bulk_upsert_template ||= File.expand_path(File.join(__FILE__, '..', 'bulk_upsert.psql.erb'))
    end

    def consolidate_dimension_template
      @consolidate_dimension_template ||= File.expand_path(File.join(__FILE__, '..', 'consolidate_dimension.psql.erb'))
    end

    def relabel_dimension_template
      @relabel_dimension_template ||= File.expand_path(File.join(__FILE__, '..', 'relabel_dimension.psql.erb'))
    end
  end
end
