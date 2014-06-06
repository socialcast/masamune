require 'spec_helper'

describe Masamune::Commands::RetryWithBackoff do
  let(:options) { {retries: retries, backoff: 0} }
  let(:delegate) { double }
  let(:instance) { described_class.new(delegate, options) }

  before do
    allow(delegate).to receive(:logger).and_return(double)
    allow(delegate).to receive(:configuration).and_return(double(retries: 0, backoff: 0))
  end

  describe '#around_execute' do
    let(:retries) { 3 }

    context 'when retry command fails with status but eventually succeeds' do
      before do
        expect(instance.logger).to receive(:error).with('exited with code: 42').exactly(retries - 1)
        expect(instance.logger).to receive(:debug).with(/retrying.*/).exactly(retries - 1)
        subject
      end

      subject do
        @retry_count = 0
        instance.around_execute do
          @retry_count += 1
          if @retry_count < retries
            OpenStruct.new(:success? => false, :exitstatus => 42)
          else
            OpenStruct.new(:success? => true)
          end
        end
      end

      it 'logs useful debug and error messages' do; end
      it 'attempts to retry the specified number of times' do
        expect(@retry_count).to eq(retries)
      end
      it 'returns result status' do
        is_expected.to be_success
      end
    end

    context 'when retry command fails with exception but eventually succeeds' do
      before do
        expect(instance.logger).to receive(:error).with('wtf').exactly(retries - 1)
        expect(instance.logger).to receive(:debug).with(/retrying.*/).exactly(retries - 1)
        subject
      end

      subject do
        @retry_count = 0
        instance.around_execute do
          @retry_count += 1
          raise 'wtf' if @retry_count < retries
          OpenStruct.new(:success? => true)
        end
      end

      it 'logs useful debug and error messages' do; end
      it 'attempts to retry the specified number of times' do
        expect(@retry_count).to eq(retries)
      end
      it 'returns result status' do
        is_expected.to be_success
      end
    end

    context 'when retry command eventually fails' do
      before do
        expect(instance.logger).to receive(:error).with('wtf').exactly(retries + 1)
        expect(instance.logger).to receive(:debug).with(/retrying.*/).exactly(retries)
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

      it 'logs useful debug and error messages' do; end
      it 'attempts to retry the specified number of times' do
        expect(@retry_count).to eq(retries + 1)
      end
      it 'returns failure status' do
        is_expected.not_to be_success
      end
    end
  end
end
