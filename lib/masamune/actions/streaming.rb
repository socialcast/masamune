module Masamune::Actions
  module Streaming
    def streaming(opts = {})
      opts = opts.to_hash.symbolize_keys

      opts.merge!(jobflow: Masamune.configuration.jobflow)

      command = if opts[:jobflow]
        Masamune::Commands::Streaming.new(opts.merge(quote: true, file_args: false))
      else
        Masamune::Commands::Streaming.new(opts)
      end

      command = Masamune::Commands::ElasticMapReduce.new(command, opts) if opts[:jobflow]
      command = Masamune::Commands::RetryWithBackoff.new(command, opts)
      command = Masamune::Commands::Shell.new(command, opts)
      command.execute
    end
  end
end
