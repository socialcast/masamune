module Masamune::Actions
  module Hive
    def hive(opts = {}, &block)
      opts = opts.to_hash.symbolize_keys

      jobflow = opts[:jobflow] || Masamune.configuration.jobflow

      opts.merge!(block: block.to_proc) if block_given?

      command = if jobflow
        Masamune::Commands::Hive.new(opts.merge(quote: true))
      else
        Masamune::Commands::Hive.new(opts)
      end

      command = if jobflow
        Masamune::Commands::ElasticMapReduce.new(command, jobflow: jobflow)
      else
        command
      end

      command = Masamune::Commands::LineFormatter.new(command, opts)
      command = Masamune::Commands::RetryWithBackoff.new(command)
      command = Masamune::Commands::Shell.new(command, fail_fast: opts.fetch(:fail_fast, false), safe: opts.fetch(:safe, false))

      if command.interactive?
        command.replace
      else
        command.execute
      end
    end
  end
end
