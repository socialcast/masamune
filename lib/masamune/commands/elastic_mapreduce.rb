module Masamune::Commands
  class ElasticMapReduce
    require 'masamune/proxy_delegate'
    include Masamune::ProxyDelegate

    attr_accessor :jobflow, :input

    def initialize(delegate, opts = {})
      @delegate    = delegate
      self.jobflow = opts[:jobflow]
      self.input   = opts[:input]
    end

    def interactive?
      if @delegate.respond_to?(:interactive?)
        @delegate.interactive?
      else
        input == nil
      end
    end

    def command_args
      args = ['elastic-mapreduce', '--jobflow', jobflow, '--ssh']
      if @delegate.respond_to?(:command_args)
        args << %Q{"#{@delegate.command_args.join(' ')}"}
      end
      args
    end

    def proxy_methods
      [:command_args, :interactive?]
    end
  end
end
