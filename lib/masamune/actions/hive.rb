require 'active_support/concern'

require 'masamune/transform/define_schema'

module Masamune::Actions
  module Hive
    include Masamune::Transform::DefineSchema

    extend ActiveSupport::Concern

    def hive(opts = {}, &block)
      opts = opts.to_hash.symbolize_keys
      opts.merge!(block: block.to_proc) if block_given?

      command = Masamune::Commands::Hive.new(environment, opts)
      command = Masamune::Commands::ElasticMapReduce.new(command, opts.except(:extra)) if configuration.elastic_mapreduce[:jobflow]
      command = Masamune::Commands::RetryWithBackoff.new(command, opts)
      command = Masamune::Commands::Shell.new(command, opts)

      command.interactive? ? command.replace : command.execute
    end

    # TODO warn or error if database is not defined
    def create_hive_database_if_not_exists
      return if configuration.hive[:database] == 'default'
      sql = []
      sql << %Q(CREATE DATABASE IF NOT EXISTS #{configuration.hive[:database]})
      sql << %Q(LOCATION "#{configuration.hive[:location]}") if configuration.hive[:location]
      hive(exec: sql.join(' ') + ';', database: nil)
    end

    def load_hive_schema
      transform = define_schema(catalog, :hive)
      rendered_file = transform.to_file
      filesystem.copy_file_to_dir(rendered_file, filesystem.get_path(:tmp_dir))
      hive(file: filesystem.get_path(:tmp_dir, File.basename(rendered_file)))
    rescue => e
      logger.error(e)
      logger.error("Could not load schema")
      logger.error("\n" + transform.to_s)
      exit
    end

    included do |base|
      base.after_initialize do |thor, options|
        thor.create_hive_database_if_not_exists
        if options[:dry_run]
          raise ::Thor::InvocationError, 'Dry run of hive failed' unless thor.hive(exec: 'SHOW TABLES;', safe: true, fail_fast: false).success?
        end
        thor.load_hive_schema
      end if defined?(base.after_initialize)
    end
  end
end
