require 'active_support/concern'

module Masamune::Actions
  module ElasticMapreduce
    extend ActiveSupport::Concern

    def elastic_mapreduce(opts = {})
      opts = opts.to_hash.symbolize_keys
      opts.reverse_merge!(configuration[:elastic_mapreduce]) if configuration[:elastic_mapreduce]

      command = Masamune::Commands::Interactive.new(:interactive => opts.fetch(:interactive, false))
      command = Masamune::Commands::ElasticMapReduce.new(command, opts)
      command = Masamune::Commands::RetryWithBackoff.new(command, opts)
      command = Masamune::Commands::Shell.new(command, opts)
      command.client = client

      command.interactive? ? command.replace : command.execute
    end

    included do |base|
      base.after_initialize do |thor, options|
        next unless Masamune.configuration.elastic_mapreduce_enabled?
        # FIXME attribute of elastic_mapreduce configuration
        jobflow = Masamune.configuration.jobflow
        raise ::Thor::RequiredArgumentMissingError, "No value provided for required options '--jobflow'" unless jobflow if thor.extra.empty?
        raise ::Thor::RequiredArgumentMissingError, %Q(Value '#{jobflow}' for '--jobflow' doesn't exist) unless thor.elastic_mapreduce(extra: '--list', jobflow: jobflow, fail_fast: false).success?
      end if defined?(base.after_initialize)
    end
  end
end
