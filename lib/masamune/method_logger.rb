# FIXME refactor as Command
module Masamune
  class MethodLogger < Delegator
    def initialize(target, *methods)
      super(target)
      @target = target
      @methods = methods
    end

    def __getobj__
      @target
    end

    def __setobj__(obj)
      @target = obj
    end

    def method_missing(method_name, *args, &block)
      @target.context.console("#{method_name} with #{args.join(' ')}") if @methods.include?(method_name)
      if @target.respond_to?(method_name)
        @target.__send__(method_name, *args, &block)
      else
        super
      end
    end
  end
end
