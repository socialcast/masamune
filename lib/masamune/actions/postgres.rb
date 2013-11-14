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
end
