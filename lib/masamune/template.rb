module Masamune
  module Template
    # From: http://stoneship.org/essays/erb-and-the-context-object/
    class ERBContext
      def initialize(hash)
        hash.each_pair do |key, value|
          instance_variable_set('@' + key.to_s, value)
        end
      end

      def get_binding
        binding
      end
    end

    class << self
      # TODO include module for magic include
      def generate(template, parameters = {})
        output = ERB.new(File.read(template)).result(ERBContext.new(parameters).get_binding)
        Tempfile.new('masamune').tap do |file|
          file.write(output)
          file.close
        end.path
      end
    end
  end
end
