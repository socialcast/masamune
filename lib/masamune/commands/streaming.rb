module Masamune::Commands
  class Streaming
    attr_accessor :input, :output, :mapper, :reducer, :extra_args, :file_args, :quote

    def initialize(opts = {})
      self.input      = Array.wrap(opts[:input])
      self.output     = opts[:output]
      self.mapper     = opts[:mapper]
      self.reducer    = opts[:reducer]
      self.extra_args = opts.fetch(:extra_args, [])
      self.file_args  = opts.fetch(:file_args, true)
      self.quote      = opts.fetch(:quote, false)
    end

    def command_args
      args = []
      args << Masamune.configuration.hadoop_streaming[:path]
      args << ['jar', Masamune.configuration.hadoop_streaming[:jar]]
      args << (quote ? extra_args.map { |arg| quote_arg(arg) } : extra_args)
      args << Masamune.configuration.hadoop_streaming[:options].map(&:to_a)
      args << ['-input', *input]
      args << ['-mapper', mapper]
      args << ['-file', mapper] if file_args
      args << ['-reducer', reducer]
      args << ['-file', reducer] if file_args
      args << ['-output', output]
      args.flatten
    end

    def before_execute
      self.input.reject! do |path|
        if Masamune.filesystem.exists?(path)
          false
        else
          Masamune.logger.debug("Removing missing input #{path} from streaming command")
          true
        end
      end
      self.input.map! do |path|
        if path =~ /part_.*\Z/ || path =~ /\..*\Z/
          path
        elsif path =~ %r{/\Z}
          path + '*'
        else
          path + '/*'
        end
      end
      Masamune.print("streaming %s -> %s (%s/%s)" % [input.join(' '), output, mapper, reducer])
    end

    def around_execute(&block)
      Dir.chdir(Masamune.filesystem.path(:var_dir)) do
        yield
      end
    end

    private

    # FIXME shell quoting is a separate concern
    def quote_arg(arg)
      out = arg.dup
      out.gsub!(%q('\t'), %q('"'\\\\\\\\t'"'))
      out
    end
  end
end
