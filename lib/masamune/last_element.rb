#  The MIT License (MIT)
#
#  Copyright (c) 2014-2015, VMware, Inc. All Rights Reserved.
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in
#  all copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
#  THE SOFTWARE.

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
          new_method = "#{method}_with_last_element"
          old_method = "#{method}_without_last_element"
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
