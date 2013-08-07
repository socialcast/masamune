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

        def missing_sources
          self.class.data_plan.missing_sources(current_command_name)
        end

        def existing_targets
          self.class.data_plan.existing_targets(current_command_name)
        end

        # TODO allow multiple after_initialize blocks
        def after_initialize
          raise Thor::RequiredArgumentMissingError, "No value provided for required options '--start'" unless options[:start] || options[:sources] || options[:targets]
          raise Thor::MalformattedArgumentError, "Cannot specify both option '--sources' and option '--targets'" if options[:sources] && options[:targets]

          self.class.data_plan.prepare current_command_name,
              start:    parse_datetime_type(:start),
              stop:     parse_datetime_type(:stop),
              sources:  parse_file_type(:sources),
              targets:  parse_file_type(:targets)

          self.class.data_plan.execute current_command_name, options
        end
      end
    end

    def parse_datetime_type(key)
      value = options[key]
      Chronic.parse(value).tap do |datetime_value|
        Masamune::print("Using '#{datetime_value}' for --#{key}") if value != datetime_value
      end or raise Thor::MalformattedArgumentError, "Expected date time value for '--#{key}'; got #{value}"
    end

    def parse_file_type(key)
      value = options[key]
      File.read(value).split(/\s+/) or raise Thor::MalformattedArgumentError, "Expected file value for '--#{key}'; got #{value}"
    end

    def current_command_name
      "#{self.class.namespace}:#{@_initializer.last[:current_command].name}"
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
        @@data_plan ||= Masamune::DataPlanBuilder.build_via_thor(@@namespaces, @@commands, @@sources, @@targets)
      end

      private

      # If internal call to Thor::Base.start fails, exit
      def exit_on_failure?
        true
      end
    end
  end
end
