require 'delegate'
require 'forwardable'

module Masamune
  module HasContext
    extend Forwardable

    def context
      @context ||= Masamune::Context.new
    end

    def context=(context)
      @context = context
    end

    def_delegators :context, :configure, :configuration, :with_exclusive_lock, :logger, :filesystem, :filesystem=, :trace, :console
  end
end
