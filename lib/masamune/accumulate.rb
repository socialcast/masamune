module Masamune
  module Accumulate
    def accumulate(method, accumulator, *args)
      accumulator.call(self, *args).tap do |elems|
        send(method, *args) do |elem|
          elems << elem
        end
      end
    end

    module ClassMethods
      def method_accumulate(method, accumulator = lambda { |_, *args| Array.new })
        self.class_eval do
          new_method = "#{method}_with_accumulate".to_sym
          old_method = "#{method}_without_accumulate".to_sym
          alias_method old_method, method
          define_method(new_method) do |*args, &block|
            if block
              send(old_method, *args, &block)
            else
              accumulate(old_method, accumulator, *args)
            end
          end
          alias_method method, new_method
        end
      end
    end

    def self.included(base)
      base.extend ClassMethods
    end
  end
end
