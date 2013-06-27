module Masamune::Commands
  class ElasticMapReduce
    require 'masamune/proxy_delegate'
    include Masamune::ProxyDelegate

    attr_accessor :jobflow, :input, :extra

    def initialize(delegate, opts = {})
      @delegate    = delegate
      self.jobflow = opts[:jobflow]
      self.input   = opts[:input]
      self.extra   = opts.fetch(:extra, [])
    end

    def interactive?
      if @delegate.respond_to?(:interactive?)
        @delegate.interactive?
      elsif extra.any?
        true
      else
        input == nil
      end
    end

    def command_args
      args = []
      args << Masamune.configuration.elastic_mapreduce[:path]
      args << Masamune.configuration.elastic_mapreduce[:options].map(&:to_a)
      args << ['--jobflow', jobflow] if jobflow
      args << extra
      if @delegate.respond_to?(:command_args)
        args << '--ssh'
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
