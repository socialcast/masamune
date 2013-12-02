require 'chronic'
require 'active_support/concern'

module Masamune::Actions
  module DataFlow
    extend ActiveSupport::Concern

    include Masamune::Actions::DateParse

    def data_plan
      self.class.data_plan
    end

    def targets
      data_plan.targets(current_command_name)
    end

    def sources
      data_plan.sources(current_command_name)
    end

    # TODO sources from file or input array
    def parse_file_type(key, default)
      return default unless key
      value = options[key] or return default
      File.exists?(value) or raise Thor::MalformattedArgumentError, "Expected file value for '--#{key}'; got #{value}"
      Set.new File.read(value).split(/\s+/)
    end

    private

    included do |base|
      base.extend ClassMethods
      base.class_eval do
        class_option :sources, :desc => 'File of data sources to process'
        class_option :targets, :desc => 'File of data targets to process'
        class_option :no_resolve, :type => :boolean, :desc => 'Do not attempt to recursively resolve data dependencies', :default => false
      end

      base.after_initialize(:final) do |thor, options|
        # Only execute this block if DataPlan is not currently executing
        next if thor.data_plan.current_rule.present?
        thor.data_plan.context = thor.context

        raise Thor::RequiredArgumentMissingError, "No value provided for required options '--start'" unless options[:start] || options[:sources] || options[:targets]
        raise Thor::MalformattedArgumentError, "Cannot specify both option '--sources' and option '--targets'" if options[:sources] && options[:targets]

        desired_sources = thor.parse_file_type(:sources, Set.new)
        desired_targets = thor.parse_file_type(:targets, Set.new)

        if thor.start_datetime && thor.stop_datetime
          desired_targets.merge thor.data_plan.targets_for_date_range(thor.current_command_name, thor.start_datetime, thor.stop_datetime)
        end

        thor.data_plan.prepare(thor.current_command_name, sources: desired_sources, targets: desired_targets)
        thor.data_plan.execute(thor.current_command_name, options)
        exit 0
      end if defined?(base.after_initialize)
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
