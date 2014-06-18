require 'spec_helper'

describe Masamune::Commands::HadoopFilesystem do
  let(:configuration) { {options: options} }
  let(:options) { [] }
  let(:attrs) { {} }

  let(:delegate) { double }
  let(:instance) { described_class.new(delegate, attrs) }

  before do
    allow(delegate).to receive_message_chain(:configuration, :hadoop_filesystem).and_return(configuration)
  end

  describe '#command_args' do
    let(:attrs) { {extra: ['-ls', '/']} }

    subject { instance.command_args }

    it { is_expected.to eq(['hadoop', 'fs', '-ls', '/']) }

    context 'with options' do
      let(:options) { [{'--conf' => 'hadoop.conf'}] }

      it { is_expected.to eq(['hadoop', 'fs', '--conf', 'hadoop.conf', '-ls', '/']) }
    end
  end
end
