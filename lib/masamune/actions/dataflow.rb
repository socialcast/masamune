module Masamune::Actions
  module Dataflow

    def input
      inputs[self.class.to_s]
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
      def source(source)
        sources[name] = source
        bind
      end

      def target(target)
        targets[name] = target
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
          data_plan.add_rule(targets[name], sources[name], name) do |file|
            Masamune::filesystem.exists? file
          end
        end
      end
    end
  end
end
