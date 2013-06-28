require 'spec_helper'

describe Masamune::Commands::S3Cmd do
  let(:general_options) { {backoff: 0} }
  let(:command_options) { [] }
  let(:context_options) { {} }

  before do
    Masamune.configuration.s3cmd[:options] = command_options
  end

  let(:instance) { Masamune::Commands::S3Cmd.new(general_options.merge(context_options)) }

  describe '#command_args' do
    let(:context_options) { {extra: ['ls', 's3://fake']} }

    subject { instance.command_args }

    it { should == ['s3cmd', 'ls', 's3://fake'] }

    context 'with command_options' do
      let(:command_options) { [{'--config' => '/opt/etc/etl/s3cfg'}] }

      it { should == ['s3cmd', '--config', '/opt/etc/etl/s3cfg', 'ls', 's3://fake'] }
    end
  end

  describe '#around_execute' do
    let(:max) { described_class::MAX_RETRIES }
    before do
      @retry_count = 0
      instance.around_execute do
        @retry_count += 1
        raise 'wtf' if @retry_count < max
      end
    end

    it 'attempts to retry a maximum number of times' do
      @retry_count.should == max
    end
  end
end
