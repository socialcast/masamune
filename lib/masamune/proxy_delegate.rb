module Masamune
  module ProxyDelegate
    def respond_to?(meth)
      methods.include?(meth) || @delegate.respond_to?(meth)
    end

    def method_missing(meth, *args, &block)
      if @delegate.respond_to?(meth)
        @delegate.send(meth, *args, &block)
      end
    end
  end
end
