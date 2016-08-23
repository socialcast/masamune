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

describe Masamune::Actions::HadoopFilesystem do
  let(:klass) do
    Class.new do
      include Masamune::HasEnvironment
      include Masamune::Actions::HadoopFilesystem
    end
  end

  let(:instance) { klass.new }

  describe '.hadoop_filesystem' do
    subject(:action) { instance.hadoop_filesystem }

    context 'when success' do
      before do
        mock_command(/\Ahadoop/, mock_success)
      end

      it { is_expected.to be_success }
    end

    context 'when failure' do
      before do
        mock_command(/\Ahadoop/, mock_failure)
      end

      it { is_expected.not_to be_success }
    end

    context 'with retries and backoff' do
      before do
        allow(instance).to receive_message_chain(:configuration, :commands, :hadoop_filesystem).and_return(retries: 1, backoff: 10)
        expect(Masamune::Commands::RetryWithBackoff).to receive(:new).with(anything, hash_including(retries: 1, backoff: 10)).once.and_call_original
        mock_command(/\Ahadoop/, mock_success)
      end

      it { is_expected.to be_success }
    end
  end
end
