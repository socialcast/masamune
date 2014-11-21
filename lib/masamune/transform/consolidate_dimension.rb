module Masamune::Transform
  class ConsolidateDimension
    def initialize(target)
      @target = target
    end

    def consolidate_dimension_as_psql
      Masamune::Template.render_to_string(consolidate_dimension_template, target: Target.new(@target))
    end

    def bulk_upsert_as_psql
      Masamune::Template.render_to_string(bulk_upsert_template, source: @target.stage_table, target: @target)
    end

    def relabel_dimension_as_psql
      RelabelDimension.new(@target).as_psql
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

  class ConsolidateDimension::Target < Delegator
    include Masamune::LastElement

    def initialize(delegate)
      @delegate = delegate
    end

    def __getobj__
      @delegate
    end

    def __setobj__(obj)
      @delegate = obj
    end

    def insert_columns(source = nil)
      consolidated_columns.map { |_, column| column.name }
    end

    def insert_view_values
      consolidated_columns.map { |_, column| column.name }
    end

    def insert_view_constraints
      consolidated_columns.reject { |_, column| column.null }.map { |_, column| "#{column.name} IS NOT NULL" }
    end
    method_with_last_element :insert_view_constraints

    def window(*extra)
      (columns.values.select { |column| extra.delete(column.name) || column.natural_key || column.auto_reference }.map(&:name) + extra).uniq
    end

    def insert_values(opts = {})
      window = opts[:window]
      consolidated_columns.map do |_, column|
        if column.natural_key
          "#{column.name} AS #{column.name}"
        elsif column.type == :key_value
          "hstore_merge(#{column.name}_now) OVER #{window} - hstore_merge(#{column.name}_was) OVER #{window} AS #{column.name}"
        else
          "coalesce_merge(#{column.name}) OVER #{window} AS #{column.name}"
        end
      end
    end
    method_with_last_element :insert_values

    private

    def consolidated_columns
      unreserved_columns.reject { |_, column| column.surrogate_key }
    end
  end
end
