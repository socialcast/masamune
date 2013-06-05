require 'masamune/commands/shell'

module Masamune::Commands
  class Streaming
    attr_accessor :input, :output, :mapper, :reducer, :extra_args, :file_args, :quote

    # FIXME remove input paths that do not exist, warn
    def initialize(opts = {})
      self.input      = opts[:input]
      self.output     = opts[:output]
      self.mapper     = opts[:mapper]
      self.reducer    = opts[:reducer]
      self.extra_args = opts.fetch(:extra_args, [])
      self.file_args  = opts.fetch(:file_args, true)
      self.quote      = opts.fetch(:quote, false)
    end

    def command_args
      args = ['hadoop', 'jar', Masamune.configuration.hadoop_streaming_jar]
      args << Masamune.configuration.command_options[:streaming].call
      args << quote ? extra_args.map { |arg| quote_arg(arg) } : extra_args
      args << ['-input', input]
      args << ['-mapper', mapper]
      args << ['-file', mapper] if file_args
      args << ['-reducer', reducer]
      args << ['-file', reducer] if file_args
      args << ['-output', output]
      args.flatten
    end

    def before_execute
      Masamune.print("streaming %s -> %s (%s/%s)" % [input, output, mapper, reducer])
    end

    def around_execute(&block)
      Dir.chdir(Masamune.filesystem.path(:var_dir)) do
        yield
      end
    end

    private

    # FIXME quoting is a separate concern
    def quote_arg(arg)
      arg.gsub("'", %q("'"))
    end
  end
end
