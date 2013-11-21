module Masamune::Commands
  class Interactive
    def initialize(attrs = {})
      @interactive = attrs.fetch(:interactive, false)
    end

    def interactive?
      @interactive
    end
  end
end
