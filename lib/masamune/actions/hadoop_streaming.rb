module Masamune::Actions
  module HadoopStreaming
    def hadoop_streaming(opts = {})
      opts = opts.to_hash.symbolize_keys

      command = if configuration.elastic_mapreduce[:jobflow]
        Masamune::Commands::HadoopStreaming.new(context, opts.merge(quote: true, upload: false))
      else
        Masamune::Commands::HadoopStreaming.new(context, opts)
      end

      command = Masamune::Commands::ElasticMapReduce.new(command, opts.except(:extra)) if configuration.elastic_mapreduce[:jobflow]
      command = Masamune::Commands::RetryWithBackoff.new(command, opts)
      command = Masamune::Commands::Shell.new(command, opts)

      command.execute
    end
  end
end
