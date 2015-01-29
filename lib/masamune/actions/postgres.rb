require 'active_support/concern'
require 'masamune/transform/define_schema'
require 'masamune/actions/postgres_admin'

module Masamune::Actions
  module Postgres
    extend ActiveSupport::Concern

    include Masamune::Actions::PostgresAdmin
    include Masamune::Transform::DefineSchema

    def postgres(opts = {}, &block)
      opts = opts.to_hash.symbolize_keys
      opts.merge!(block: block.to_proc) if block_given?

      command = Masamune::Commands::Postgres.new(environment, opts)
      command = Masamune::Commands::Shell.new(command, opts)

      command.interactive? ? command.replace : command.execute
    end

    def create_database_if_not_exists
      unless postgres_helper.database_exists?
        postgres_admin(action: :create, database: configuration.postgres[:database], safe: true)
      end if configuration.postgres.has_key?(:database)
    end

    def load_setup_files
      configuration.postgres[:setup_files].each do |file|
        configuration.with_quiet do
          postgres(file: file)
        end
      end if configuration.postgres.has_key?(:setup_files)
    end

    def load_schema_registry
      transform = define_schema(registry, :postgres)
      postgres(file: transform.to_file)
    rescue => e
      logger.error(e)
      logger.error("Could not load schema")
      logger.error("\n" + transform.to_s)
      exit
    end

    included do |base|
      base.after_initialize do |thor, options|
        thor.create_database_if_not_exists
        thor.load_setup_files
        thor.load_schema_registry
      end if defined?(base.after_initialize)
    end
  end
end
