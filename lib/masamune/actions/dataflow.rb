module Masamune::Actions
  module Dataflow
    private

    def self.included(base)
      base.extend ClassMethods
      base.class_eval do
        attr_accessor :sources, :targets

        class_option :start, :aliases => '-a', :desc => 'Start time', :default => nil
        class_option :stop, :aliases => '-b', :desc => 'Stop time', :default => Date.today.to_s
        class_option :sources, :type => :array, :desc => 'Input to process'

        private

        def sources=(source_paths)
          self.class.data_plan.sources_from_paths(current_command_name, source_paths)
        end

        def targets=(target_paths)
          self.class.data_plan.targets_from_paths(current_command_name, target_paths)
        end

        def targets
          return @targets if @targets
          @sources.map do |source|
            self.class.data_plan.targets_for_source(current_command_name, source)
          end.flatten.reject do |target|
            if fs.exists?(target.path)
              Masamune::print("skipping existing #{target.path}")
              true
            else
              false
            end
          end
        end

        # TODO allow multiple after_initialize blocks
        def after_initialize
          raise Thor::RequiredArgumentMissingError, "No value provided for required options '--start'" unless options[:start] || options[:sources] || options[:targets]
          raise %q(Cannot specify both option '--sources' and option '--targets') if options[:sources] && options[:targets]

          sources = options[:sources] if options[:sources]
          targets = options[:targets] if options[:targets]

          unless sources || targets
            start = DateTime.parse(options[:start])
            stop = DateTime.parse(options[:stop])

            # TODO encapsulate
            if targets = self.class.data_plan.targets_for_date_range(current_command_name, start, stop)
              unless self.class.data_plan.resolve(current_command_name, targets, options)
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
          # TODO try invoke
          self.start([command_name(source_options), '--sources', *sources] + command_options)
        end

        # TODO trigger indepenent of order
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
