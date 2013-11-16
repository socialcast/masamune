module Masamune::Actions
  module Postgres
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
  end

  # TODO load setup_files
  # TODO load schema_files
  after_register do |base|
    # FIXME
    next unless (class << base; self; end).included_modules.include?(Masamune::Actions::Postgres)
    base.extend(Masamune::Actions::PostgresAdmin)
    configuration = Masamune.configuration.postgres
    unless base.postgres(exec: 'SELECT version();', fail_fast: false).success?
      base.postgres_admin(action: :create, database: configuration[:database])
    end
  end
end
