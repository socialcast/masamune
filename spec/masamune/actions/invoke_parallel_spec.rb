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

describe Masamune::Actions::InvokeParallel do
  let(:klass) do
    Class.new(Thor) do
      include Masamune::HasEnvironment
      include Masamune::Actions::InvokeParallel
    end
  end

  let(:instance) { klass.new }
  let(:task_opts) { { max_tasks: 0 }.freeze }

  describe '.invoke_parallel' do
    context 'with a single thor command' do
      before do
        mock_command(/\Athor list/, mock_success)
      end

      subject do
        instance.invoke_parallel('list', task_opts)
      end

      it { expect { subject }.to_not raise_error }
    end

    context 'with a single thor command and multiple arguments' do
      before do
        mock_command(/\Athor list --a/, mock_success)
        mock_command(/\Athor list --b/, mock_success)
      end

      subject do
        instance.invoke_parallel('list', task_opts, [{ a: true, b: false }, { a: false, b: true }])
      end

      it { expect { subject }.to_not raise_error }
    end

    context 'with a single thor command and multiple environments' do
      before do
        mock_command(/\AMASAMUNE_ENV=test_1 thor list/, mock_success)
        mock_command(/\AMASAMUNE_ENV=test_2 thor list/, mock_success)
      end

      subject do
        instance.invoke_parallel('list', task_opts, [{ env: { 'MASAMUNE_ENV' => 'test_1' } }, { env: { 'MASAMUNE_ENV' => 'test_2' } }])
      end

      it { expect { subject }.to_not raise_error }
    end

    context 'with a multiple thor command and multiple environments' do
      before do
        mock_command(/\AMASAMUNE_ENV=test_1 thor list/, mock_success)
        mock_command(/\AMASAMUNE_ENV=test_2 thor list/, mock_success)
        mock_command(/\AMASAMUNE_ENV=test_1 thor help/, mock_success)
        mock_command(/\AMASAMUNE_ENV=test_2 thor help/, mock_success)
      end

      subject do
        instance.invoke_parallel('list', 'help', task_opts, [{ env: { 'MASAMUNE_ENV' => 'test_1' } }, { env: { 'MASAMUNE_ENV' => 'test_2' } }])
      end

      it { expect { subject }.to_not raise_error }
    end
  end
end
