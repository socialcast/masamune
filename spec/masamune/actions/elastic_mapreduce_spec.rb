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

describe Masamune::Actions::ElasticMapreduce do
  let(:klass) do
    Class.new do
      include Masamune::HasEnvironment
      include Masamune::AfterInitializeCallbacks
      include Masamune::Actions::ElasticMapreduce
    end
  end

  let(:instance) { klass.new }
  let(:configuration) { {} }
  let(:extra) { [] }

  before do
    allow(instance).to receive_message_chain(:configuration, :elastic_mapreduce).and_return(configuration)
    allow(instance).to receive(:extra).and_return(extra)
  end

  describe '.elastic_mapreduce' do
    before do
      mock_command(/\Aelastic-mapreduce/, mock_success)
    end

    subject { instance.elastic_mapreduce }

    it { is_expected.to be_success }

    context 'with retries and backoff' do
      before do
        allow(instance).to receive_message_chain(:configuration, :elastic_mapreduce).and_return(retries: 1, backoff: 10)
        expect(Masamune::Commands::RetryWithBackoff).to receive(:new).with(anything, hash_including(retries: 1, backoff: 10)).once.and_call_original
      end

      it { is_expected.to be_success }
    end
  end

  describe '.after_initialize' do
    let(:options) { {initialize: true} }

    subject(:after_initialize_invoke) do
      instance.after_initialize_invoke(options)
    end

    context 'when configuration is empty' do
      it { expect { subject }.to_not raise_error }
    end

    context 'when jobflow not required due to extra options' do
      let(:configuration) { {enabled: true} }
      let(:extra) { ['--create', '--name', 'zombo_cluster'] }
      it { expect { subject }.to_not raise_error }
    end

    context 'when jobflow is missing' do
      let(:configuration) { {enabled: true} }
      it { expect { subject }.to raise_error Thor::RequiredArgumentMissingError, /No value provided for required options '--jobflow'/ }
    end

    context 'when jobflow is present without initialize' do
      let(:configuration) { {enabled: true} }
      let(:options) { {jobflow: 'j-XYZ'} }
      before do
        expect(instance).to_not receive(:elastic_mapreduce)
      end
      it do
        expect { subject }.to_not raise_error
        expect(instance.configuration.elastic_mapreduce[:jobflow]).to eq('j-XYZ')
      end
    end

    context 'when jobflow does not exist' do
      let(:configuration) { {enabled: true} }
      let(:options) { {initialize: true, jobflow: 'j-XYZ'} }
      before do
        mock_command(/\Aelastic-mapreduce/, mock_failure)
      end
      it { expect { subject }.to raise_error Thor::RequiredArgumentMissingError, /Value 'j-XYZ' for '--jobflow' doesn't exist/ }
    end

    context 'when jobflow exists' do
      let(:configuration) { {enabled: true} }
      let(:options) { {initialize: true, jobflow: 'j-XYZ'} }
      before do
        mock_command(/\Aelastic-mapreduce/, mock_success)
      end
      it do
        expect { subject }.to_not raise_error
        expect(instance.configuration.elastic_mapreduce[:jobflow]).to eq('j-XYZ')
      end
    end

    context 'when jobflow is symbolic' do
      let(:configuration) { {enabled: true, jobflows: {'build' => 'j-XYZ'}} }
      let(:options) { {initialize: true, jobflow: 'build', } }
      before do
        mock_command(/\Aelastic-mapreduce/, mock_success)
      end
      it do
        expect { subject }.to_not raise_error
        expect(instance.configuration.elastic_mapreduce[:jobflow]).to eq('j-XYZ')
      end
    end
  end
end
