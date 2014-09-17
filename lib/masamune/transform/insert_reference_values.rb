module Masamune::Transform
  class InsertReferenceValues
    def initialize(source, target)
      @source = source
      @target = target
    end

    def as_psql
      templates = []
      @target.insert_references.each do |_, reference|
        templates << Masamune::Template.render_to_string(template, source: @source, target: Target.new(reference))
      end
      templates.join("\n")
    end

    private

    def template
      @template ||= File.expand_path(File.join(__FILE__, '..', 'insert_reference_values.psql.erb'))
    end
  end

  class InsertReferenceValues::Target < Delegator
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
      source.shared_columns(stage_table).map { |_, columns| columns.first.name }
    end

    def insert_values(source)
      source.shared_columns(stage_table).map do |column, _|
        if column.adjacent.try(:default)
          "COALESCE(#{column.name}, #{column.adjacent.sql_value(column.adjacent.default)})"
        else
          column.name
        end
      end
    end
    method_with_last_element :insert_values

    def insert_constraints(source)
      source.shared_columns(stage_table).reject { |column, _| column.null || column.default || column.adjacent.try(:default) }.map { |column, _| "#{column.name} IS NOT NULL"}
    end
    method_with_last_element :insert_constraints
  end
end
