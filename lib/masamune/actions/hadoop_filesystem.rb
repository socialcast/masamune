module Masamune::Actions
  module HadoopFilesystem
    def hadoop_filesystem(*args, &block)
      opts = args.last.is_a?(Hash) ? args.pop : {}
      opts = opts.to_hash.symbolize_keys
      opts.reverse_merge!(configuration.hadoop_filesystem) if configuration
      opts.reverse_merge!(fail_fast: false)
      opts.merge!(extra: Array.wrap(args))
      opts.merge!(block: block.to_proc) if block_given?

      command = Masamune::Commands::HadoopFilesystem.new(context, opts)
      command = Masamune::Commands::RetryWithBackoff.new(command, opts)
      command = Masamune::Commands::Shell.new(command, opts)

      command.execute
    end
    alias hadoop_fs hadoop_filesystem
  end
end
