require 'spec_helper'

describe Masamune::Commands::RetryWithBackoff do
  let(:general_options) { {retries: retries, backoff: 0} }
  let(:command_options) { [] }
  let(:context_options) { {} }

  let(:delegate) { mock }
  let(:instance) { Masamune::Commands::RetryWithBackoff.new(delegate, general_options.merge(context_options)) }

  describe '#around_execute' do
    let(:retries) { 3 }

    context 'when retry command eventually succeeds' do
      before do
        Masamune.logger.should_receive(:error).with('wtf').exactly(retries - 1)
        Masamune.logger.should_receive(:debug).with(/retrying.*/).exactly(retries - 1)
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
        Masamune.logger.should_receive(:error).with('wtf').exactly(retries + 1)
        Masamune.logger.should_receive(:debug).with(/retrying.*/).exactly(retries)
        Masamune.logger.should_receive(:debug).with(/max retries.*bailing/)
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
