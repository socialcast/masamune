module Masamune
  class MethodLogger < Delegator
    def initialize(target, options = {})
      super(target)
      @target = target
      @ignore = options[:ignore]
    end

    def __getobj__
      @target
    end

    def __setobj__(obj)
      @target = obj
    end

    def method_missing(method_name, *args, &block)
      Masamune::print("#{method_name} with #{args.join(' ')}") unless @ignore.include?(method_name)
      if @target.respond_to?(method_name)
        @target.__send__(method_name, *args, &block)
      else
        super
      end
    end
  end
end
