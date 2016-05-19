#  The MIT License (MIT)
#
#  Copyright (c) 2014-2016, VMware, Inc. All Rights Reserved.
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

describe Masamune::AfterInitializeCallbacks do
  let(:klass) do
    Class.new do
      include Masamune::AfterInitializeCallbacks

      def first_callback; end

      def early_callback; end

      def default_callback; end

      def unknown_callback; end

      def later_callback; end

      def final_callback; end

      after_initialize(:first, &:first_callback)
      after_initialize(:early, &:early_callback)
      after_initialize(:default, &:default_callback)
      after_initialize(:unknown, &:unknown_callback)
      after_initialize(:later, &:later_callback)
      after_initialize(:final, &:final_callback)
    end
  end

  let(:instance) { klass.new }

  describe '.after_initialize_invoke' do
    before do
      expect(instance).to receive(:first_callback).once.ordered
      expect(instance).to receive(:early_callback).once.ordered
      expect(instance).to receive(:default_callback).once.ordered
      expect(instance).to receive(:unknown_callback).once.ordered
      expect(instance).to receive(:later_callback).once.ordered
      expect(instance).to receive(:final_callback).once.ordered
      instance.after_initialize_invoke
    end

    it 'should call callbacks in priority order' do
    end
  end
end
