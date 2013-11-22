module Masamune::Actions
  module HadoopFilesystem
    def hadoop_filesystem(*args, &block)
      opts = args.last.is_a?(Hash) ? args.pop : {}
      opts = opts.to_hash.symbolize_keys
      opts.reverse_merge!(configuration.hadoop_filesystem) if configuration
      opts.reverse_merge!(fail_fast: false)
      opts.merge!(extra: Array.wrap(args))
      opts.merge!(block: block.to_proc) if block_given?

      command = if opts[:jobflow]
        Masamune::Commands::HadoopFilesystem.new(opts.merge(quote: true, file_args: false))
      else
        Masamune::Commands::HadoopFilesystem.new(opts)
      end

      command = Masamune::Commands::ElasticMapReduce.new(command, opts) if opts[:jobflow]
      command = Masamune::Commands::RetryWithBackoff.new(command, opts)
      command = Masamune::Commands::Shell.new(command, opts)
      command.context = context

      command.execute
    end
    alias hadoop_fs hadoop_filesystem
  end
end
