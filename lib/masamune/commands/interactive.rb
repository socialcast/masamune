require 'masamune/proxy_delegate'

module Masamune::Commands
  class Interactive
    include Masamune::ProxyDelegate

    def initialize(delegate, attrs = {})
      @delegate = delegate
      @interactive = attrs.fetch(:interactive, false)
    end

    def interactive?
      @interactive
    end
  end
end
