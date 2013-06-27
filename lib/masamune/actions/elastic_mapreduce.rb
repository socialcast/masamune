module Masamune::Actions
  module ElasticMapreduce
    def elastic_mapreduce(opts = {})
      opts = opts.to_hash.symbolize_keys
      opts.merge!(jobflow: Masamune.configuration.jobflow)

      command = Masamune::Commands::Interactive.new(:interactive => opts.fetch(:interactive, false))
      command = Masamune::Commands::ElasticMapReduce.new(command, opts)
      command = Masamune::Commands::Shell.new(command, fail_fast: opts.fetch(:fail_fast, false), input: opts[:input])

      if command.interactive?
        command.replace
      else
        command.execute
      end
    end
  end
end
