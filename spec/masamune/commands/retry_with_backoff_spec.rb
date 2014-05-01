require 'spec_helper'

describe Masamune::Commands::RetryWithBackoff do
  let(:options) { {retries: retries, backoff: 0} }
  let(:delegate) { double }
  let(:instance) { described_class.new(delegate, options) }

  before do
    delegate.stub(:logger).and_return(double)
  end

  describe '#around_execute' do
    let(:retries) { 3 }

    context 'when retry command fails with status but eventually succeeds' do
      before do
        instance.logger.should_receive(:error).with('exited with code: 42').exactly(retries - 1)
        instance.logger.should_receive(:debug).with(/retrying.*/).exactly(retries - 1)
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
        @retry_count.should == retries
      end
      it 'returns result status' do
        should be_success
      end
    end

    context 'when retry command fails with exception but eventually succeeds' do
      before do
        instance.logger.should_receive(:error).with('wtf').exactly(retries - 1)
        instance.logger.should_receive(:debug).with(/retrying.*/).exactly(retries - 1)
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
        @retry_count.should == retries
      end
      it 'returns result status' do
        should be_success
      end
    end

    context 'when retry command eventually fails' do
      before do
        instance.logger.should_receive(:error).with('wtf').exactly(retries + 1)
        instance.logger.should_receive(:debug).with(/retrying.*/).exactly(retries)
        instance.logger.should_receive(:debug).with(/max retries.*bailing/)
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
        @retry_count.should == retries + 1
      end
      it 'returns failure status' do
        should_not be_success
      end
    end
  end
end
