module Masamune::Transform
  class DefineEventView
    def initialize(source, target)
      @source = source
      @target = target
    end

    def as_hql
      Masamune::Template.render_to_string(template, source: @source, target: @target)
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
end
