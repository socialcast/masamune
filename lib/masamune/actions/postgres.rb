require 'active_support/concern'
require 'masamune/actions/postgres_admin'

module Masamune::Actions
  module Postgres
    extend ActiveSupport::Concern
    include Masamune::Actions::PostgresAdmin

    def postgres(opts = {}, &block)
      opts = opts.to_hash.symbolize_keys

      opts.merge!(block: block.to_proc) if block_given?

      command = Masamune::Commands::Postgres.new(opts)
      command = Masamune::Commands::LineFormatter.new(command, opts)
      command = Masamune::Commands::Shell.new(command, opts)

      if command.interactive?
        command.replace
      else
        command.execute
      end
    end

    included do |base|
      base.after_initialize do |thor, options|
        configuration = thor.configuration.postgres
        unless thor.postgres(exec: 'SELECT version();', fail_fast: false).success?
          thor.postgres_admin(action: :create, database: configuration[:database])
        end
        configuration[:setup_files].each do |file|
          thor.postgres(file: file)
        end
        configuration[:schema_files].each do |file|
          thor.postgres(file: file)
        end
      end if defined?(base.after_initialize)
    end
  end
end