module Masamune::Actions
  module Hive
    def hive(opts = {}, &block)
      opts = opts.to_hash.symbolize_keys

      jobflow = opts[:jobflow] || Masamune.configuration.jobflow

      opts.merge!(block: block.to_proc) if block_given?

      command = Masamune::Commands::Hive.new(opts)

      command = if jobflow
        Masamune::Commands::ElasticMapReduce.new(command, jobflow: jobflow)
      else
        command
      end

      command = Masamune::Commands::LineFormatter.new(command, opts)
      command = Masamune::Commands::RetryWithBackoff.new(command, opts)
      command = Masamune::Commands::Shell.new(command, opts)

      if command.interactive?
        command.replace
      else
        command.execute
      end
    end
  end
end
