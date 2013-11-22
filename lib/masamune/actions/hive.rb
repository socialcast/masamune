require 'active_support/concern'

module Masamune::Actions
  module Hive
    extend ActiveSupport::Concern

    def hive(opts = {}, &block)
      opts = opts.to_hash.symbolize_keys
      opts.reverse_merge!(configuration.hive) if configuration
      opts.merge!(block: block.to_proc) if block_given?

      command = Masamune::Commands::Hive.new(context, opts)
      command = Masamune::Commands::ElasticMapReduce.new(command, opts.reverse_merge(configuration.elastic_mapreduce)) if configuration.elastic_mapreduce[:jobflow]
      command = Masamune::Commands::LineFormatter.new(command, opts)
      command = Masamune::Commands::RetryWithBackoff.new(command, opts)
      command = Masamune::Commands::Shell.new(command, opts)

      command.interactive? ? command.replace : command.execute
    end

    included do |base|
      base.after_initialize do |thor, options|
        if options[:dry_run]
          raise ::Thor::InvocationError, 'Dry run of hive failed' unless thor.hive(exec: 'show tables;', safe: true, fail_fast: false).success?
        end
      end if defined?(base.after_initialize)
    end
  end
end
