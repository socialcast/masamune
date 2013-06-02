module Masamune::Actions
  module Streaming
    include Masamune::Actions::Common

    def streaming(opts, args)
      Masamune.print("streaming %s -> %s (%s/%s)" % [opts[:input], opts[:output], opts[:mapper], opts[:reducer]])

      Dir.chdir(Masamune.configuration.var_dir) do
        if jobflow = Masamune.configuration.jobflow || opts[:jobflow]
          execute(*elastic_mapreduce_ssh(jobflow, 'hadoop', 'jar', Masamune.configuration.hadoop_streaming_jar, *args, *elastic_mapreduce_streaming_args(opts)), :fail_fast => true)
        else
          execute('hadoop', 'jar', Masamune.configuration.hadoop_streaming_jar, *args, *streaming_args(opts), :fail_fast => true)
        end
      end
    end

    private

    def streaming_args(opts)
      args = []
      args << ['-input', opts[:input]]
      args << ['-mapper', opts[:mapper], '-file', opts[:mapper]]
      args << ['-reducer', opts[:reducer], '-file', opts[:reducer]]
      args << ['-output', opts[:output]]
      args.flatten
    end

    def elastic_mapreduce_streaming_args(opts)
      args = []
      args << ['-input', opts[:input]]
      args << ['-mapper', opts[:mapper]]
      args << ['-reducer', opts[:reducer]]
      args << ['-output', opts[:output]]
      args.flatten
    end

    def elastic_mapreduce_ssh(jobflow, *args)
      ['elastic-mapreduce', '--jobflow', jobflow, '--ssh', %Q{"#{args.join(' ')}"}]
    end
  end
end
