module Masamune::Commands
  class ElasticMapReduce
    require 'masamune/proxy_delegate'
    include Masamune::ProxyDelegate

    attr_accessor :jobflow, :input, :mode

    def initialize(delegate, opts = {})
      @delegate    = delegate
      self.jobflow = opts[:jobflow]
      self.input   = opts[:input]
      self.mode    = opts.fetch('mode', '--ssh')
    end

    def interactive?
      if @delegate.respond_to?(:interactive?)
        @delegate.interactive?
      else
        input == nil
      end
    end

    def command_args
      args = []
      args << 'elastic-mapreduce'
      args << Masamune.configuration.elastic_mapreduce[:options].to_a
      args << ['--jobflow', jobflow] if jobflow
      args << mode
      if @delegate.respond_to?(:command_args)
        args << %Q{"#{@delegate.command_args.join(' ')}"}
      end
      args.flatten
    end

    def handle_stdout(line, line_no)
      if line_no == 0 && line =~ /\Assh/
        @delegate.handle_stderr(line, line_no) if @delegate.respond_to?(:handle_stderr)
      else
        @delegate.handle_stdout(line, line_no) if @delegate.respond_to?(:handle_stdout)
      end
    end

    def proxy_methods
      [:command_args, :interactive?, :handle_stdout]
    end
  end
end
