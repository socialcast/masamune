module Masamune::Actions
  module PostgresAdmin
    def postgres_admin(opts = {}, &block)
      opts = opts.to_hash.symbolize_keys
      opts.reverse_merge!(configuration.postgres) if configuration
      opts.reverse_merge!(configuration.postgres_admin) if configuration

      command = Masamune::Commands::PostgresAdmin.new(context, opts)
      command = Masamune::Commands::Shell.new(command, opts)

      command.execute
    end
  end
end
