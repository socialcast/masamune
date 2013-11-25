require 'delegate'
require 'forwardable'
require 'active_support/concern'

module Masamune
  module HasContext
    extend Forwardable
    extend ActiveSupport::Concern

    module ClassMethods
      def context
        @context ||= Masamune::Context.new
      end

      def context=(context)
        @context = context
      end
    end
    include ClassMethods

    def_delegators :context, :configure, :configuration, :with_exclusive_lock, :logger, :filesystem, :filesystem=, :trace, :console
  end
end
