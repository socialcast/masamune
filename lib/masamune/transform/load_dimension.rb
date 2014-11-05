module Masamune::Transform
  class LoadDimension
    def initialize(source_file, source, target)
      @source_file = source_file
      @target = target.type == :four ? target.ledger_table : target
      @source = source.as_table(@target)
    end

    def stage_dimension_as_psql
      Masamune::Template.render_to_string(stage_dimension_template, source: @source, source_file: @source_file)
    end

    def load_dimension_as_psql
      Masamune::Template.render_to_string(load_dimension_template, source: @source, target: Target.new(@target))
    end

    def insert_reference_values_as_psql
      InsertReferenceValues.new(@source, @target).as_psql
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

    def load_dimension_template
      @load_dimension_template ||= File.expand_path(File.join(__FILE__, '..', 'load_dimension.psql.erb'))
    end
  end

  class LoadDimension::Target < Delegator
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

    def insert_columns(source)
      shared_columns(source).values.map do |columns|
        column = columns.first
        if reference = column.reference
          reference.foreign_key_name
        else
          column.name
        end
      end
    end

    def insert_values(source)
      shared_columns(source).values.map do |columns|
        column = columns.first
        if reference = column.reference
          reference.primary_key.qualified_name(reference.label)
        elsif column.type == :json || column.type == :yaml || column.type == :key_value
          "json_to_hstore(#{column.qualified_name})"
        else
          column.qualified_name
        end
      end
    end
    method_with_last_element :insert_values

    def join_conditions(source)
      join_columns = shared_columns(source).values.flatten.lazy
      join_columns = join_columns.select { |column| column.reference }.lazy
      join_columns = join_columns.group_by { |column| column.reference }.lazy

      conditions = Hash.new { |h,k| h[k] = Set.new }
      join_columns.each do |reference, columns|
        left_uniq = Set.new
        (columns + lateral_references(source, reference)).each do |column|
          left = reference.columns[column.id]
          # FIXME hash on left.qualified_name(reference.label) to prevent duplicates
          next unless left_uniq.add?(left.qualified_name(reference.label))
          conditions[[reference.name, reference.alias]] << "#{left.qualified_name(reference.label)} = #{column.qualified_name}"
        end
      end
      conditions
    end

    def lateral_references(source, reference)
      source.shared_columns(reference).keys.reject { |column| column.auto_reference }
    end
  end
end
