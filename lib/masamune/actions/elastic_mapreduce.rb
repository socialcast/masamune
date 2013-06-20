module Masamune::Actions
  require 'masamune/commands/shell'
  require 'masamune/commands/interactive'
  require 'masamune/commands/elastic_mapreduce'

  module ElasticMapreduce
    def elastic_mapreduce(opts = {})
      opts = opts.to_hash.symbolize_keys
      opts.merge!(jobflow: Masamune.configuration.jobflow)
      opts[:extra_args] ||= []
      opts[:extra_args] << '--list' if opts[:list]
      opts[:extra_args] << '--help' if opts[:help]

      command = Masamune::Commands::Interactive.new(:interactive => opts.fetch(:interactive, false))
      command = Masamune::Commands::ElasticMapReduce.new(command, opts)
      command = Masamune::Commands::Shell.new(command, fail_fast: opts.fetch(:fail_fast, false))

      if command.interactive? || opts[:replace]
        command.replace
      else
        command.execute
      end
    end
  end
end
