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
      defined_jobflows.fetch(jobflow.to_sym, jobflow.to_s)
    end

    included do |base|
      base.class_option :jobflow, :aliases => '-j', :desc => 'Elastic MapReduce jobflow ID (Hint: elastic-mapreduce --list)' if defined?(base.class_option)
      base.after_initialize do |thor, options|
        next if thor.configuration.elastic_mapreduce.empty?
        next unless thor.configuration.elastic_mapreduce.fetch(:enabled, true)
        jobflow = options[:jobflow] if options[:jobflow]
        jobflow ||= thor.configuration.elastic_mapreduce[:jobflow]
        raise ::Thor::RequiredArgumentMissingError, "No value provided for required options '--jobflow'" unless jobflow if thor.extra.empty?
        jobflow = thor.resolve_jobflow(jobflow)
        raise ::Thor::RequiredArgumentMissingError, %Q(Value '#{jobflow}' for '--jobflow' doesn't exist) unless thor.elastic_mapreduce(extra: '--list', jobflow: jobflow, fail_fast: false).success?
        thor.configuration.elastic_mapreduce[:jobflow] = jobflow
      end if defined?(base.after_initialize)
    end
  end
end
