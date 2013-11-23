require 'spec_helper'

describe Masamune::Commands::S3Cmd do
  let(:configuration) { {:options => options} }
  let(:options) { [] }
  let(:attrs) { {} }

  let(:delegate) { double }
  let(:instance) { described_class.new(delegate, attrs) }

  before do
    delegate.stub_chain(:configuration, :s3cmd).and_return(configuration)
  end

  describe '#command_args' do
    let(:attrs) { {extra: ['ls', 's3://fake']} }

    subject { instance.command_args }

    it { should == ['s3cmd', 'ls', 's3://fake'] }

    context 'with options' do
      let(:options) { [{'--config' => '/opt/etc/etl/s3cfg'}] }

      it { should == ['s3cmd', '--config', '/opt/etc/etl/s3cfg', 'ls', 's3://fake'] }
    end
  end
end
