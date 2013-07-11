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
        @retry_count = 0
        instance.around_execute do
          @retry_count += 1
          raise 'wtf' if @retry_count < retries
        end
      end

      it 'logs useful debug and error messages' do; end
      it 'attempts to retry the specified number of times' do
        @retry_count.should == retries
      end
    end

    context 'when retry command eventually fails' do
      before do
        Masamune.logger.should_receive(:error).with('wtf').exactly(retries + 1)
        Masamune.logger.should_receive(:debug).with(/retrying.*/).exactly(retries)
        Masamune.logger.should_receive(:debug).with(/max retries.*bailing/)
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
      it 'eventually bails' do; end
    end
  end
end
