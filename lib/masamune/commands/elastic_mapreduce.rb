module Masamune::Commands
  class ElasticMapReduce
    attr_accessor :jobflow, :input

    def initialize(delegate, opts = {})
      @delegate    = delegate
      self.jobflow = opts[:jobflow]
      self.input   = opts[:input]
    end

    def interactive?
      input == nil
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

    def respond_to?(meth)
      proxy_methods.include?(meth) || @delegate.respond_to?(meth)
    end

    def method_missing(meth, *args)
      if @delegate.respond_to?(meth)
        @delegate.send(meth, *args)
      end
    end
  end
end
