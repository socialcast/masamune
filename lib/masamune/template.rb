require 'tilt'

module Masamune
  class Template
    def initialize(paths = [])
      @paths = Array.wrap(paths)
    end

    def render(template, parameters = {})
      Tilt.new(resolve_file(template)).render(self, parameters)
    end

    private

    # TODO unify with resolve_path
    def resolve_file(partial_file)
      @paths.map do |path|
        file = File.join(path, File.basename(partial_file))
        file if File.exists?(file) && File.file?(file)
      end.compact.first
    end

    class << self
      def render_to_file(template, parameters = {})
        instance = Template.new(File.dirname(template))
        Tempfile.new('masamune').tap do |file|
          file.write(instance.render(template, parameters))
          file.close
        end.path
      end
    end
  end
end
