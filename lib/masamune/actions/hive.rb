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

    # TODO warn or error if database is not defined
    def create_database_if_not_exists
      sql = []
      sql << %Q(CREATE DATABASE IF NOT EXISTS #{configuration.hive[:database]})
      sql << %Q(LOCATION "#{configuration.hive[:location]}") if configuration.hive[:location]
      hive(exec: sql.join(' ') + ';', database: nil)
    end

    included do |base|
      base.after_initialize do |thor, options|
        thor.create_database_if_not_exists
        if options[:dry_run]
          raise ::Thor::InvocationError, 'Dry run of hive failed' unless thor.hive(exec: 'SHOW TABLES;', safe: true, fail_fast: false).success?
        end
        # thor.load_setup_and_schema_files
      end if defined?(base.after_initialize)
    end
  end
end
