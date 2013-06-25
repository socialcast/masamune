module Masamune::Commands
  class Interactive
    attr_accessor :interactive

    def initialize(opts = {})
      self.interactive = opts[:interactive]
    end

    def interactive?
      interactive
    end
  end
end
