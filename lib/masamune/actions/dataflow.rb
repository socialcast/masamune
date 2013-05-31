module Masamune::Actions
  module Dataflow
    private

    def self.included(base)
      base.extend ClassMethods
      base.class_eval do
        attr_accessor :source_paths

        class_option :start, :aliases => '-a', :desc => 'Start time', :default => nil
        class_option :stop, :aliases => '-b', :desc => 'Stop time', :default => Date.today.to_s
        class_option :sources, :type => :array, :desc => 'Input to process'

        private

        def targets
          self.source_paths.map do |input_file|
            self.class.data_plan.target_for_source(current_command_name, input_file)
          end.reject do |target_file|
            if fs.exists?(target_file.path)
              Masamune::print("skipping exsiting #{target_file.path}")
              true
            else
              false
            end
          end
        end

        # TODO allow multiple after_initialize blocks
        def after_initialize
          raise Thor::RequiredArgumentMissingError, "No value provided for required options '--start'" unless options[:start] || options[:sources]

          if options[:sources]
            self.source_paths = options[:sources]
          else
            start = DateTime.parse(options[:start])
            stop = DateTime.parse(options[:stop])
            if source_paths = self.class.data_plan.targets(current_command_name, start, stop)
              unless self.class.data_plan.resolve(current_command_name, source_paths, options)
                abort "No matching input files for #{current_command_name} between #{options[:start]} and #{options[:stop]}"
              end
            end

            # NOTE resolve has executed original thor task via anonymous proc - safe to exit
            exit
          end
        end
      end
    end

    def current_command_name
      @_initializer.last[:current_command].name.to_sym
    end

    module ClassMethods
      def source(source, source_options = {})
        data_plan.add_source(command_name(source_options), source, source_options)

        thor_wrapper = Proc.new do |sources, runtime_options|
          command_options = command_options(runtime_options)
          Masamune.logger.debug([command_name(source_options), '--sources', *sources] + command_options)
          self.start([command_name(source_options), '--sources', *sources] + command_options)
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
