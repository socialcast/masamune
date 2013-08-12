require 'chronic'

module Masamune::Actions
  module DataFlow
    private

    def self.included(base)
      base.extend ClassMethods
      base.class_eval do
        class_option :start, :aliases => '-a', :desc => 'Start time', :default => nil
        class_option :stop, :aliases => '-b', :desc => 'Stop time', :default => Date.today.to_s
        class_option :sources, :desc => 'File of data sources to process'
        class_option :targets, :desc => 'File of data targets to process'

        private

        def targets
          data_plan.targets(current_command_name)
        end

        def sources
          data_plan.sources(current_command_name)
        end

        # TODO messaging
        # Masamune::print("skipping missing source #{source.path}")
        # Masamune::print("skipping existing #{target.path}")

        # TODO allow multiple after_initialize blocks
        def after_initialize
          raise Thor::RequiredArgumentMissingError, "No value provided for required options '--start'" unless options[:start] || options[:sources] || options[:targets]
          raise Thor::MalformattedArgumentError, "Cannot specify both option '--sources' and option '--targets'" if options[:sources] && options[:targets]

          desired_sources = parse_file_type(:sources, Set.new)
          desired_targets = parse_file_type(:targets, Set.new)

          if options[:start] && options[:stop]
            desired_targets.merge data_plan.targets_for_date_range(current_command_name, parse_datetime_type(:start), parse_datetime_type(:stop))
          end

          Masamune.thor_instance ||= self

          if Masamune.thor_instance.current_command_name == current_command_name
            data_plan.prepare(current_command_name, sources: desired_sources, targets: desired_targets)
            data_plan.execute(current_command_name, options)
          end

          # NOTE Execution continues to original thor task
        end
      end
    end

    def data_plan
      self.class.data_plan
    end

    def parse_datetime_type(key)
      value = options[key]
      Chronic.parse(value).tap do |datetime_value|
        Masamune::print("Using '#{datetime_value}' for --#{key}") if value != datetime_value
      end or raise Thor::MalformattedArgumentError, "Expected date time value for '--#{key}'; got #{value}"
    end

    # TODO sources from file or input array
    def parse_file_type(key, default)
      return default unless key
      value = options[key] or return default
      File.exists?(value) or raise Thor::MalformattedArgumentError, "Expected file value for '--#{key}'; got #{value}"
      Set.new File.read(value).split(/\s+/)
    end

    module ClassMethods
      def source(source, loadtime_options = {})
        @@namespaces ||= []
        @@namespaces << namespace
        @@sources ||= []
        @@sources << [source, loadtime_options]
      end

      def target(target, loadtime_options = {})
        @@targets ||= []
        @@targets << [target, loadtime_options]
      end

      def create_command(*a)
        super.tap do
          @@commands ||= []
          @@commands << a
        end
      end

      def data_plan
        @@data_plan ||= Masamune::DataPlanBuilder.instance.build(@@namespaces, @@commands, @@sources, @@targets)
      end

      private

      # If internal call to Thor::Base.start fails, exit
      def exit_on_failure?
        true
      end
    end
  end
end
