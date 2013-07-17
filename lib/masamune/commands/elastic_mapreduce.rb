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

    def stdin
      if @delegate.respond_to?(:input)
        @delegate.stdin
      elsif input
        @stdin ||= StringIO.new(input)
      end
    end

    def command_args
      args = []
      args << Masamune.configuration.elastic_mapreduce[:path]
      args << Masamune.configuration.elastic_mapreduce[:options].map(&:to_a)
      args << ['--jobflow', jobflow] if jobflow
      args << extra
      args << '--ssh' if ssh_command?
      args << %Q{"#{@delegate.command_args.join(' ')}"} if @delegate.respond_to?(:command_args)
      args.flatten
    end

    def handle_stdout(line, line_no)
      if line_no == 0 && line =~ /\Assh/
        @delegate.handle_stderr(line, line_no) if @delegate.respond_to?(:handle_stderr)
      else
        @delegate.handle_stdout(line, line_no) if @delegate.respond_to?(:handle_stdout)
      end
    end

    private

    def ssh_command?
      @delegate.respond_to?(:command_args) || input
    end
  end
end
