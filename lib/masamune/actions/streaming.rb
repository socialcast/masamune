module Masamune::Actions
  module Streaming
    include Masamune::Actions::Common

    def streaming(opts, args)
      Masamune.print("streaming %s -> %s (%s/%s)" % [opts[:input], opts[:output], opts[:mapper], opts[:reducer]])

      Dir.chdir(Masamune.configuration.var_dir) do
        execute('hadoop', 'jar', Masamune.configuration.hadoop_streaming_jar, *args,
                '-input', opts[:input],
                '-mapper', opts[:mapper], '-file', opts[:mapper],
                '-reducer', opts[:reducer], '-file', opts[:reducer],
                '-output', opts[:output], :fail_fast => true)
      end
    end
  end
end
