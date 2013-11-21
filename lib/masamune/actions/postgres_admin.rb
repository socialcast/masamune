module Masamune::Actions
  module PostgresAdmin
    def postgres_admin(opts = {}, &block)
      opts = opts.to_hash.symbolize_keys
      opts.reverse_merge!(configuration[:postgres]) if configuration[:postgres]
      opts.reverse_merge!(configuration[:postgres_admin]) if configuration[:postgres_admin]

      command = Masamune::Commands::PostgresAdmin.new(opts)
      command = Masamune::Commands::Shell.new(command, opts)
      command.client = client

      command.execute
    end
  end
end
