require 'active_support/concern'
require 'masamune/actions/postgres_admin'

module Masamune::Actions
  module Postgres
    extend ActiveSupport::Concern
    include Masamune::Actions::PostgresAdmin

    def postgres(opts = {}, &block)
      opts = opts.to_hash.symbolize_keys
      opts.merge!(block: block.to_proc) if block_given?

      command = Masamune::Commands::Postgres.new(context, opts)
      command = Masamune::Commands::LineFormatter.new(command, opts)
      command = Masamune::Commands::Shell.new(command, opts)

      command.interactive? ? command.replace : command.execute
    end

    def create_database_if_not_exists
      unless postgres(exec: 'SELECT version();', fail_fast: false).success?
        postgres_admin(action: :create, database: configuration.postgres[:database])
      end if configuration.postgres.has_key?(:database)
    end

    def load_setup_files
      configuration.postgres[:setup_files].each do |file|
        postgres(file: file)
      end if configuration.postgres.has_key?(:setup_files)
    end

    def load_schema_files
      configuration.postgres[:schema_files].each do |file|
        postgres(file: file)
      end if configuration.postgres.has_key?(:schema_files)
    end

    included do |base|
      base.after_initialize do |thor, options|
        thor.create_database_if_not_exists
        thor.load_setup_files
        thor.load_schema_files
      end if defined?(base.after_initialize)
    end
  end
end
