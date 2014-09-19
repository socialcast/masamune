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
      source.columns.map do |_, column|
        if reference = column.reference
          reference.foreign_key_name
        else
          column.name
        end
      end
    end

    def insert_values(source)
      source.columns.map do |_, column|
        if reference = column.reference
          constraints = {}
          constraints[column.foreign_key_name] = column.name
          shared_columns(reference).each do |left_column, right_columns|
            next if left_column.auto_reference
            right_columns.each do |right_column|
              constraints[right_column.qualified_name] = left_column.name
            end
          end

          where_clause = constraints.map { |key, value| "#{key} = #{value}" }.join(' AND ')
          "(SELECT #{reference.primary_key.name} FROM #{reference.name} WHERE #{where_clause})"
        elsif column.type == :json || column.type == :yaml || column.type == :key_value
          "json_to_hstore(#{column.name})"
        else
          column.name.to_s
        end
      end
    end
    method_with_last_element :insert_values
  end
end
