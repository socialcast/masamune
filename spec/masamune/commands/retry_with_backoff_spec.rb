require 'spec_helper'

describe Masamune::Commands::RetryWithBackoff do
  let(:general_options) { {retries: retries, backoff: 0} }
  let(:command_options) { [] }
  let(:context_options) { {} }

  let(:delegate) { mock }
  let(:instance) { Masamune::Commands::RetryWithBackoff.new(delegate, general_options.merge(context_options)) }

  describe '#around_execute' do
    let(:retries) { 3 }

    before do
      @retry_count = 0
      instance.around_execute do
        @retry_count += 1
        raise 'wtf' if @retry_count < retries
      end
    end

    it 'attempts to retry the specified number of times' do
      @retry_count.should == retries
    end
  end
end
