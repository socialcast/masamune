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

describe Masamune::Actions::AwsEmr do
  let(:klass) do
    Class.new do
      include Masamune::HasEnvironment
      include Masamune::AfterInitializeCallbacks
      include Masamune::Actions::AwsEmr
    end
  end

  let(:instance) { klass.new }
  let(:configuration) { {} }

  before do
    allow(instance).to receive_message_chain(:configuration, :commands, :aws_emr).and_return(configuration)
  end

  describe '.aws_emr' do
    subject(:action) { instance.aws_emr }

    context 'when success' do
      before do
        mock_command(/\Aaws emr/, mock_success)
      end

      it { is_expected.to be_success }
    end

    context 'when failure' do
      before do
        mock_command(/\Aaws emr/, mock_failure)
      end

      it { expect { action }.to raise_error RuntimeError, 'fail_fast: aws emr ssh' }
    end

    context 'with retries and backoff' do
      before do
        allow(instance).to receive_message_chain(:configuration, :commands, :aws_emr).and_return(retries: 1, backoff: 10)
        expect(Masamune::Commands::RetryWithBackoff).to receive(:new).with(anything, hash_including(retries: 1, backoff: 10)).once.and_call_original
        mock_command(/\Aaws emr/, mock_success)
      end

      it { is_expected.to be_success }
    end
  end

  describe '.after_initialize' do
    let(:options) { { initialize: true } }

    subject(:after_initialize_invoke) do
      instance.after_initialize_invoke(options)
    end

    context 'when configuration is empty' do
      it { expect { subject }.to_not raise_error }
    end

    context 'when cluster_id is missing' do
      let(:configuration) { { enabled: true } }
      it { expect { subject }.to raise_error Thor::RequiredArgumentMissingError, /No value provided for required options '--cluster-id'/ }
    end

    context 'when cluster_id is present without initialize' do
      let(:configuration) { { enabled: true } }
      let(:options) { { cluster_id: 'j-XYZ' } }
      before do
        expect(instance).to_not receive(:aws_emr)
      end
      it do
        expect { subject }.to_not raise_error
        expect(instance.configuration.commands.aws_emr[:cluster_id]).to eq('j-XYZ')
      end
    end

    context 'when cluster_id does not exist' do
      let(:configuration) { { enabled: true } }
      let(:options) { { initialize: true, cluster_id: 'j-XYZ' } }
      before do
        mock_command(/\Aaws emr/, mock_failure)
      end
      it { expect { subject }.to raise_error Thor::RequiredArgumentMissingError, /AWS EMR cluster 'j-XYZ' does not exist/ }
    end

    context 'when cluster_id exists' do
      let(:configuration) { { enabled: true } }
      let(:options) { { initialize: true, cluster_id: 'j-XYZ' } }
      before do
        mock_command(/\Aaws emr/, mock_success)
      end
      it do
        expect { subject }.to_not raise_error
        expect(instance.configuration.commands.aws_emr[:cluster_id]).to eq('j-XYZ')
      end
    end
  end
end
