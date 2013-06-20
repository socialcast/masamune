module Masamune::Actions
  require 'masamune/commands/shell'
  require 'masamune/commands/elastic_mapreduce'
  require 'masamune/commands/streaming'

  module Streaming
    def streaming(opts = {})
      opts = opts.to_hash.symbolize_keys

      jobflow = opts[:jobflow] || Masamune.configuration.jobflow

      command = if jobflow
        Masamune::Commands::Streaming.new(opts.merge(quote: true, file_args: false))
      else
        Masamune::Commands::Streaming.new(opts)
      end

      command = if jobflow
        Masamune::Commands::ElasticMapReduce.new(command, jobflow: jobflow)
      else
        command
      end

      command = Masamune::Commands::Shell.new(command, fail_fast: true)
      command.execute
    end
  end
end
