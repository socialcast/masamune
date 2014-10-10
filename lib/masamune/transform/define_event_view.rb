module Masamune::Transform
  class DefineEventView
    def initialize(source, target)
      @source = source
      @target = target
    end

    def as_hql
      Masamune::Template.render_to_string(template, source: @source, target: Target.new(@target))
    end

    def to_hql_file
      Tempfile.new('masamune').tap do |file|
        file.write(as_hql)
        file.close
      end.path
    end

    private

    def template
      @template ||= File.expand_path(File.join(__FILE__, '..', 'define_event_view.hql.erb'))
    end
  end

  class DefineEventView::Target < Delegator
    def initialize(delegate)
      @delegate = delegate
    end

    def __getobj__
      @delegate
    end

    def __setobj__(obj)
      @delegate = obj
    end

    def view_name
      "#{id}_events"
    end

    def view_columns
      unreserved_columns.map do |_, column|
        column.name
      end
    end

    def view_values
      unreserved_columns.map do |_, column|
        case column.type
        when :json
          # NOTE could just use split "\t" to parse tsv output
          %Q{CONCAT('"', REGEXP_REPLACE(#{column.name}, '"', '""'),  '"') AS #{column.name}}
        else
          column.name
        end
      end
    end
  end
end
