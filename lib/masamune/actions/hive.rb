require 'active_support/concern'

module Masamune::Actions
  module Hive
    extend ActiveSupport::Concern

    def hive(opts = {}, &block)
      opts = opts.to_hash.symbolize_keys
      opts.merge!(block: block.to_proc) if block_given?

      command = Masamune::Commands::Hive.new(context, opts)
      command = Masamune::Commands::ElasticMapReduce.new(command, opts) if configuration.elastic_mapreduce[:jobflow]
      command = Masamune::Commands::LineFormatter.new(command, opts)
      command = Masamune::Commands::RetryWithBackoff.new(command, opts)
      command = Masamune::Commands::Shell.new(command, opts)

      command.interactive? ? command.replace : command.execute
    end

    def load_setup_files
      configuration.hive[:setup_files].each do |file|
        hive(file: file)
      end if configuration.hive.has_key?(:setup_files)
    end

    def load_schema_files
      configuration.hive[:schema_files].each do |file|
        hive(file: file)
      end if configuration.hive.has_key?(:schema_files)
    end

    included do |base|
      base.after_initialize do |thor, options|
        if options[:dry_run]
          raise ::Thor::InvocationError, 'Dry run of hive failed' unless thor.hive(exec: 'SHOW TABLES;', safe: true, fail_fast: false).success?
        else
          thor.load_setup_files
          thor.load_schema_files
        end
      end if defined?(base.after_initialize)
    end
  end
end
