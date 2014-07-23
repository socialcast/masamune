require 'active_support/concern'
require 'masamune/actions/postgres_admin'

module Masamune::Actions
  module Postgres
    extend ActiveSupport::Concern
    include Masamune::Actions::PostgresAdmin

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

    def load_schema_files
      configuration.postgres[:schema_files].each do |path|
        filesystem.glob_sort(path, order: :basename).each do |file|
          configuration.with_quiet do
            if file =~ /\.rb\Z/
              registry.load(file)
            else
              postgres(file: file)
            end
          end
        end
      end if configuration.postgres.has_key?(:schema_files)
    end

    def load_schema_registry
      return if registry.empty?
      postgres(file: registry.to_file)
    rescue
      logger.error("Could not load schema from registry")
      logger.error("\n" + registry.to_s)
      exit
    end

    included do |base|
      base.after_initialize do |thor, options|
        thor.create_database_if_not_exists
        thor.load_setup_files
        thor.load_schema_files
        thor.load_schema_registry
      end if defined?(base.after_initialize)
    end
  end
end
