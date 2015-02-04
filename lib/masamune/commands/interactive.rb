require 'delegate'

module Masamune::Commands
  class Interactive < SimpleDelegator

    def initialize(delegate, attrs = {})
      super delegate
      @interactive = attrs.fetch(:interactive, false)
    end

    def interactive?
      @interactive
    end
  end
end
