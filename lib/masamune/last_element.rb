require 'active_support/concern'

module Masamune
  module LastElement
    extend ActiveSupport::Concern

    def last_element(method, *args)
      instance = send(method, *args)
      case instance
      when Array
        instance.map { |elem| [elem, elem == instance.last] }
      when Hash
        instance.map { |key, value| [key, value, key == instance.keys.last] }
      end
    end

    module ClassMethods
      def method_with_last_element(method)
        self.class_eval do
          new_method = "#{method}_with_last_element".to_sym
          old_method = "#{method}_without_last_element".to_sym
          alias_method old_method, method
          define_method(new_method) do |*args, &block|
            last_element(old_method,  *args)
          end
          alias_method method, new_method
        end
      end
    end
  end
end
