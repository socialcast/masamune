module Masamune::Actions
  module Dataflow
    def input_files
      inputs[self.class.to_s].to_a
    end

    private

    def self.included(base)
      base.extend ClassMethods
    end

    def inputs
      @inputs ||= begin
        start = DateTime.parse(options[:start])
        stop = DateTime.parse(options[:stop])
        self.class.data_plan.resolve(start, stop)
        self.class.data_plan.matches
      end
    end

    module ClassMethods
      def source(source, options = {})
        sources[name] = [source, options]
        bind
      end

      def target(target, options = {})
        targets[name] = [target, options]
        bind
      end

      def data_plan
        @data_plan ||= Masamune::DataPlan.new
      end

      private

      def sources
        @sources ||= {}
      end

      def targets
        @targets ||= {}
      end

      def bind
        if sources[name] && targets[name]
          data_plan.add_rule(*targets[name], *sources[name], name) do |file|
            # TODO invididual checks too slow - need to generate entire target tree, use in memory lookup
            Masamune::filesystem.exists? file
          end
        end
      end
    end
  end
end
