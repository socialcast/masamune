require 'active_support/concern'

module Masamune
  module AfterInitializeCallbacks
    extend ActiveSupport::Concern

    PRIORITY =
    {
      first:    20,
      early:    10,
      default:   0,
      later:   -10,
      final:   -20
    }

    module ClassMethods
      # Callbacks registered with the highest priority are executed first, ties are broken by callback registration order
      def after_initialize(priority = :default, &block)
        @after_initialize ||= Hash.new { |h,k| h[k] = [] }
        @after_initialize[PRIORITY.fetch(priority, 0)] << block
      end

      def after_initialize_invoke(*a)
        @after_initialize ||= Hash.new { |h,k| h[k] = [] }
        @after_initialize.sort.reverse.each { |p, x| x.each { |y| y.call(*a) } }
      end
    end

    def after_initialize_invoke(*a)
      self.class.after_initialize_invoke(self, *a)
    end
  end
end
