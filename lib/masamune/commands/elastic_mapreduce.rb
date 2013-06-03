module Masamune::Commands
  class ElasticMapReduce
    attr_accessor :jobflow

    def initialize(delegate, opts = {})
      @delegate = delegate
      self.jobflow = opts[:jobflow]
    end

    def command_args
      ['elastic-mapreduce', '--jobflow', jobflow, '--ssh', %Q{"#{@delegate.command_args.join(' ')}"}]
    end

    private

    def method_missing(meth, *args)
      if @delegate.respond_to?(meth)
        @delegate.send(meth, *args)
      end
    end

    def respond_to?(meth)
      @delegate.respond_to?(meth)
    end
  end
end
