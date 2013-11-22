require 'delegate'

module Masamune
  module HasContext
    extend Forwardable

    def context
      @context || Masamune.default_context
    end

    def context=(context)
      @context = context
    end

    def_delegators :context, :configure, :configuration, :with_exclusive_lock, :logger, :filesystem, :filesystem=, :trace, :print
  end
end
