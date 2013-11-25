require 'active_support/concern'
module Masamune
  module AfterInitializeCallbacks
    extend ActiveSupport::Concern

    module ClassMethods
      # Callbacks registered with the highest priority are executed first, ties are broken by callback registration order
      def after_initialize(priority = 0, &block)
        @after_initialize ||= Hash.new { |h,k| h[k] = [] }
        @after_initialize[priority] << block
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
