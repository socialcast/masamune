require 'active_support/concern'

module Masamune::Actions
  module ElasticMapreduce
    extend ActiveSupport::Concern

    def elastic_mapreduce(opts = {})
      opts = opts.to_hash.symbolize_keys
      opts[:jobflow] = resolve_jobflow(opts[:jobflow]) if opts[:jobflow]

      command = Masamune::Commands::Interactive.new(context, :interactive => opts.fetch(:interactive, false))
      command = Masamune::Commands::ElasticMapReduce.new(command, opts)
      command = Masamune::Commands::RetryWithBackoff.new(command, opts)
      command = Masamune::Commands::Shell.new(command, opts)

      command.interactive? ? command.replace : command.execute
    end

    def defined_jobflows
      @defined_jobflows ||= configuration.elastic_mapreduce.fetch(:jobflows, {}).symbolize_keys
    end

    def resolve_jobflow(jobflow)
      return unless jobflow
      defined_jobflows.fetch(jobflow.to_sym, jobflow.to_s)
    end

    def jobflow_required?
      extra.empty?
    end

    included do |base|
      base.class_option :jobflow, :aliases => '-j', :desc => 'Elastic MapReduce jobflow ID (Hint: elastic-mapreduce --list)' if defined?(base.class_option)
      base.after_initialize(5) do |thor, options|
        next if thor.configuration.elastic_mapreduce.empty?
        next unless thor.configuration.elastic_mapreduce.fetch(:enabled, true)
        jobflow = thor.resolve_jobflow(options.symbolize_keys.fetch(:jobflow, thor.configuration.elastic_mapreduce[:jobflow]))
        if thor.jobflow_required?
          raise ::Thor::RequiredArgumentMissingError, "No value provided for required options '--jobflow'" unless jobflow
          raise ::Thor::RequiredArgumentMissingError, %Q(Value '#{jobflow}' for '--jobflow' doesn't exist) unless thor.elastic_mapreduce(extra: '--list', jobflow: jobflow, fail_fast: false).success?
        end
        thor.configuration.elastic_mapreduce[:jobflow] = jobflow
      end if defined?(base.after_initialize)
    end
  end
end
