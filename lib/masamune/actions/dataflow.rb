module Masamune::Actions
  module Dataflow
    def input_files
      set = inputs[current_command_name]
      quit "No input sources to process for #{current_command_name}" if set.empty?
      set.to_a
    end

    private

    def self.included(base)
      base.extend ClassMethods
      base.class_eval do
        class_option :start, :aliases => '-a', :desc => 'Start time', :default => nil, :required => true
        class_option :stop, :aliases => '-b', :desc => 'Stop time', :default => Date.today.to_s
      end
    end

    def current_command_name
      @_initializer.last[:current_command].name.to_sym
    end

    def inputs
      @inputs ||= begin
        start = DateTime.parse(options[:start])
        stop = DateTime.parse(options[:stop])
        self.class.data_plan.resolve(start, stop, current_command_name)
        self.class.data_plan.matches
      end
    end

    def quit(a)
      say a if a
      exit
    end

    module ClassMethods
      def source(source, params = {})
        sources[command_name(params)] = [source, params]
        bind(params)
      end

      def target(target, params = {})
        targets[command_name(params)] = [target, params]
        bind(params)
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

      def bind(params)
        if sources[command_name(params)] && targets[command_name(params)]
          data_plan.add_rule(*targets[command_name(params)], *sources[command_name(params)], command_name(params))
        end
      end

      # TODO infer command_name even when explicit :for is missing
      def command_name(params = {})
        params[:for]
      end
    end
  end
end
