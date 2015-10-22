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
