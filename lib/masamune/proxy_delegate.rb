module Masamune
  module ProxyDelegate
    def proxy_methods
      []
    end

    def respond_to?(meth)
      proxy_methods.include?(meth) || methods.include?(meth) || @delegate.respond_to?(meth)
    end

    def method_missing(meth, *args, &block)
      if @delegate.respond_to?(meth)
        @delegate.send(meth, *args, &block)
      end
    end
  end
end
