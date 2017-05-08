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

describe Masamune::Actions::Execute do
  let(:klass) do
    Class.new do
      include Masamune::HasEnvironment
      include Masamune::Actions::Execute

      def before_execute
        raise
      end

      def after_execute
        raise
      end
    end
  end

  let(:instance) { klass.new }

  describe '.execute' do
    before do
      expect(Masamune::Commands::RetryWithBackoff).to receive(:new).with(anything, anything).once.and_call_original
    end

    context 'with a simple command' do
      let(:command) { %w[echo ping] }
      let(:options) { { fail_fast: true } }

      it { expect { |b| instance.execute(*command, options, &b) }.to yield_with_args('ping', 0) }
    end

    context 'with a simple command with input' do
      let(:command) { %w[cat] }
      let(:options) { { input: 'pong', fail_fast: true } }

      it { expect { |b| instance.execute(*command, options, &b) }.to yield_with_args('pong', 0) }
    end

    context 'with a simple command with env' do
      let(:command) { %(bash -c "echo $MESSAGE") }
      let(:options) { { env: { 'MESSAGE' => 'pong' }, fail_fast: true } }

      it { expect { |b| instance.execute(*command, options, &b) }.to yield_with_args('pong', 0) }
    end
  end
end
