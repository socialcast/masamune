module Masamune::Commands
  class ElasticMapReduce
    require 'masamune/proxy_delegate'
    include Masamune::ProxyDelegate

    attr_accessor :jobflow, :input, :extra_args

    def initialize(delegate, opts = {})
      @delegate    = delegate
      self.jobflow = opts[:jobflow]
      self.input   = opts[:input]
      self.extra_args = opts.fetch(:extra_args, [])
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
      args << Masamune.configuration.elastic_mapreduce[:options].map(&:to_a)
      args << ['--jobflow', jobflow] if jobflow
      args << extra_args
      if @delegate.respond_to?(:command_args) || @delegate.interactive? || input
        args << '--ssh'
      end
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
