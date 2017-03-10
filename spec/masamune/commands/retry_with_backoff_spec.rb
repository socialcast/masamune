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

describe Masamune::Commands::RetryWithBackoff do
  let(:options) { { max_retries: max_retries, backoff: 0 } }
  let(:delegate) { double }
  let(:instance) { described_class.new(delegate, options) }

  before do
    allow(delegate).to receive(:logger).and_return(double)
    allow(delegate).to receive(:configuration).and_return(double(max_retries: 0, backoff: 0))
  end

  describe '#around_execute' do
    let(:max_retries) { 3 }

    context 'when retry command fails with status but eventually succeeds' do
      before do
        expect(instance.logger).to receive(:error).with('exited with code: 42').exactly(max_retries - 1)
        expect(instance.logger).to receive(:debug).with(/retrying.*/).exactly(max_retries - 1)
        subject
      end

      subject do
        @retry_count = 0
        instance.around_execute do
          @retry_count += 1
          if @retry_count < max_retries
            OpenStruct.new(success?: false, exitstatus: 42)
          else
            OpenStruct.new(success?: true)
          end
        end
      end

      it 'logs useful debug and error messages' do
      end
      it 'attempts to retry the specified number of times' do
        expect(@retry_count).to eq(max_retries)
      end
      it 'returns result status' do
        is_expected.to be_success
      end
    end

    context 'when retry command fails with exception but eventually succeeds' do
      before do
        expect(instance.logger).to receive(:error).with('wtf').exactly(max_retries - 1)
        expect(instance.logger).to receive(:debug).with(/retrying.*/).exactly(max_retries - 1)
        subject
      end

      subject do
        @retry_count = 0
        instance.around_execute do
          @retry_count += 1
          raise 'wtf' if @retry_count < max_retries
          OpenStruct.new(success?: true)
        end
      end

      it 'logs useful debug and error messages' do
      end
      it 'attempts to retry the specified number of times' do
        expect(@retry_count).to eq(max_retries)
      end
      it 'returns result status' do
        is_expected.to be_success
      end
    end

    context 'when retry command eventually fails' do
      before do
        expect(instance.logger).to receive(:error).with('wtf').exactly(max_retries + 1)
        expect(instance.logger).to receive(:debug).with(/retrying.*/).exactly(max_retries)
        expect(instance.logger).to receive(:debug).with(/max retries.*bailing/)
        subject
      end

      subject do
        @retry_count = 0
        instance.around_execute do
          @retry_count += 1
          raise 'wtf'
        end
      end

      it 'logs useful debug and error messages' do
      end
      it 'attempts to retry the specified number of times' do
        expect(@retry_count).to eq(max_retries + 1)
      end
      it 'returns failure status' do
        is_expected.not_to be_success
      end
    end
  end
end
