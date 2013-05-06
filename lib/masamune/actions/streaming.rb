module Masamune::Actions
  module Streaming
    include Masamune::Actions::Common

    def streaming(options, args)
      Dir.chdir(Masamune.configuration.var_dir) do
        execute('hadoop', 'jar', Masamune.configuration.hadoop_streaming_jar, *args,
                '-input', options[:input],
                '-mapper', options[:mapper], '-file', options[:mapper],
                '-reducer', options[:reducer], '-file', options[:reducer],
                '-output', options[:output])
      end
    end
  end
end
