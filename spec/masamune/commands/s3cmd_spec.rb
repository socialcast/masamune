require 'spec_helper'

describe Masamune::Commands::S3Cmd do
  let(:general_options) { {} }
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
end
