module Masamune::Transform
  class RelabelDimension
    def initialize(target)
      @target = target
    end

    def as_psql
      Masamune::Template.render_to_string(template, target: Target.new(@target))
    end

    def to_psql_file
      Tempfile.new('masamune').tap do |file|
        file.write(as_psql)
        file.close
      end.path
    end

    private

    def template
      @template ||= File.expand_path(File.join(__FILE__, '..', 'relabel_dimension.psql.erb'))
    end
  end

  class RelabelDimension::Target < Delegator
    def initialize(delegate)
      @delegate = delegate
    end

    def __getobj__
      @delegate
    end

    def __setobj__(obj)
      @delegate = obj
    end

    def window(*extra)
      (columns.values.select { |column| extra.delete(column.name) || column.surrogate_key }.map(&:name) + extra).uniq
    end
  end
end
