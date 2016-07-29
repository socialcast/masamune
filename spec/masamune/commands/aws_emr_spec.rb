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

describe Masamune::Commands::AwsEmr do
  let(:configuration) { {} }
  let(:attrs) { {} }

  let(:delegate) { double }
  let(:instance) { described_class.new(delegate, attrs) }

  before do
    allow(delegate).to receive(:logger).and_return(double)
    allow(delegate).to receive_message_chain(:configuration, :commands, :aws_emr).and_return(configuration)
  end

  describe '#command_env' do
    subject { instance.command_env }
    it { is_expected.to be_empty }

    context 'with config file' do
      let(:configuration) { { config_file: '/etc/aws/config' } }
      it { is_expected.to eq('AWS_CONFIG_FILE' => '/etc/aws/config') }
    end
  end

  describe '#command_args' do
    subject { instance.command_args }

    it { is_expected.to eq(%w(aws emr ssh)) }

    context 'with --cluster-id j-XYZ' do
      let(:delegate) { double(command_args: ['hive', '-e', "'show tables;'"]) }
      let(:attrs) { { config_file: '/etc/aws_config', cluster_id: 'j-XYZ' } }

      before do
        expect(instance).to receive(:execute).with('aws', 'emr', 'ssh', '--cluster-id', 'j-XYZ', '--command', 'exit', env: { 'AWS_CONFIG_FILE' => '/etc/aws_config' }, fail_fast: true, safe: true)
          .and_yield('ssh -o StrictHostKeyChecking=no -o ServerAliveInterval=10 -i /etc/ssh/aws.key hadoop@ec2-10.0.0.1.compute-1.amazonaws.com exit')
          .and_yield("Warning: Permanently added 'ec2-10.0.0.1.compute-1.amazonaws.com,10.0.0.1' (ECDSA) to the list of known hosts.")
      end

      it { is_expected.to eq(['ssh', '-o', 'StrictHostKeyChecking=no', '-o', 'ServerAliveInterval=10', '-i', '/etc/ssh/aws.key', 'hadoop@ec2-10.0.0.1.compute-1.amazonaws.com', 'hive', '-e', "'show tables;'"]) }
    end

    context 'with action' do
      let(:configuration) { { create_cluster: { options: { '--ami-version' => '3.5.0' } } } }
      let(:attrs) { { action: 'create-cluster', extra: ['--instance-type', 'm1.large'] } }
      it { is_expected.to eq(['aws', 'emr', 'create-cluster', '--ami-version', '3.5.0', '--instance-type', 'm1.large']) }
    end

    context 'with action and option override (symbolized)' do
      let(:configuration) { { create_cluster: { options: { :'--ami-version' => '3.5.0' } } } }
      let(:attrs) { { action: 'create-cluster', extra: ['--ami-version', '4.0.0', '--instance-type', 'm1.large'] } }
      it { is_expected.to eq(['aws', 'emr', 'create-cluster', '--ami-version', '4.0.0', '--instance-type', 'm1.large']) }
    end

    context 'with action and option override (stringified)' do
      let(:configuration) { { 'create_cluster' => { 'options' => { '--ami-version' => '3.5.0' } } } }
      let(:attrs) { { action: 'create-cluster', extra: ['--ami-version', '4.0.0', '--instance-type', 'm1.large'] } }
      it { is_expected.to eq(['aws', 'emr', 'create-cluster', '--ami-version', '4.0.0', '--instance-type', 'm1.large']) }
    end

    context 'with action: wait' do
      let(:attrs) { { action: 'wait', cluster_id: 'j-XYZ' } }
      it { is_expected.to eq(['aws', 'emr', 'wait', 'cluster-running', '--cluster-id', 'j-XYZ']) }
    end
  end

  context '#interactive?' do
    subject { instance.interactive? }
    it { is_expected.to eq(false) }

    context 'with interactive: true' do
      let(:attrs) { { interactive: true } }
      it { is_expected.to eq(true) }
    end

    context 'with interactive: false' do
      let(:attrs) { { interactive: false } }
      it { is_expected.to eq(false) }
    end

    context 'when delegate.interactive? is true' do
      before do
        allow(delegate).to receive(:interactive?).and_return(true)
      end
      it { is_expected.to eq(true) }
    end

    context 'when delegate.interactive? is false' do
      before do
        allow(delegate).to receive(:interactive?).and_return(false)
      end
      it { is_expected.to eq(false) }
    end
  end
end
