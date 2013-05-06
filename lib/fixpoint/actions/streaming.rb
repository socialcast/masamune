module Fixpoint::Actions
  module Streaming
    include Fixpoint::Actions::Common

    def streaming(options, args)
      Dir.chdir(Fixpoint.configuration.var_dir) do
        execute('hadoop', 'jar', Fixpoint.configuration.hadoop_streaming_jar, *args,
                '-input', options[:input],
                '-mapper', options[:mapper], '-file', options[:mapper],
                '-reducer', options[:reducer], '-file', options[:reducer],
                '-output', options[:output])
      end
    end
  end
end
