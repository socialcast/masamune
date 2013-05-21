module Masamune::Actions
  module Dataflow
    private

    def self.included(base)
      base.extend ClassMethods
      base.class_eval do
        attr_accessor :input_files

        # TODO start + stop XOR inputs
        class_option :start, :aliases => '-a', :desc => 'Start time', :default => nil
        class_option :stop, :aliases => '-b', :desc => 'Stop time', :default => Date.today.to_s
        class_option :inputs, :type => :array, :desc => 'Input to process'

        private

        # TODO allow multiple after_initialize blocks
        def after_initialize
          self.input_files = if options[:inputs]
            options[:inputs]
          else
            start = DateTime.parse(options[:start])
            stop = DateTime.parse(options[:stop])
            self.class.data_plan.targets(current_command_name, start, stop)
          end
          # TODO allow user to remove existing targets
          self.class.data_plan.resolve(current_command_name, self.input_files, options)
        end
      end
    end

    def current_command_name
      @_initializer.last[:current_command].name.to_sym
    end

    module ClassMethods
      def source(source, source_options = {})
        data_plan.add_source(command_name(source_options), source, source_options)

        thor_wrapper = Proc.new do |inputs, runtime_options|
          command_options = command_options(runtime_options)
          Masamune.logger.debug([command_name(source_options), '--inputs', *inputs] + command_options)
          self.start([command_name(source_options), '--inputs', *inputs] + command_options)
        end

        data_plan.add_command(command_name(source_options), thor_wrapper)
      end

      def target(target, target_options = {})
        data_plan.add_target(command_name(target_options), target, target_options)
      end

      def data_plan
        @data_plan ||= Masamune::DataPlan.new
      end

      private

      # If internal call to Thor::Base.start fails, exit
      def exit_on_failure?
        true
      end

      # TODO infer command_name even when explicit :for is missing
      def command_name(options = {})
        options[:for]
      end

      def command_options(runtime_options)
        runtime_options.reject { |_,v| v == false }.map { |k,v| ["--#{k}", v == true ? nil : v] }.flatten.compact
      end
    end
  end
end
