require 'spec_helper'

describe Masamune::Actions::ElasticMapreduce do
  let(:klass) do
    Class.new do
      extend Masamune::Thor::BeforeInitializeCallbacks
      include Masamune::HasContext
      include Masamune::Actions::ElasticMapreduce
    end
  end

  let(:instance) { klass.new }
  let(:configuration) { {} }
  let(:extra) { [] }

  before do
    instance.stub_chain(:configuration, :elastic_mapreduce).and_return(configuration)
    instance.stub(:extra).and_return(extra)
  end

  describe '.elastic_mapreduce' do
    before do
      mock_command(/\Aelastic-mapreduce/, mock_success)
    end

    subject { instance.elastic_mapreduce }

    it { should be_success }
  end

  describe '.after_initialize' do
    let(:options) { {} }

    subject(:after_initialize_invoke) do
      klass.after_initialize_invoke(instance, options)
    end

    context 'when configuration is empty' do
      it { expect { subject }.to_not raise_error }
    end

    context 'when jobflow is missing' do
      let(:configuration) { {enabled: true} }
      it { expect { subject }.to raise_error Thor::RequiredArgumentMissingError, /No value provided for required options '--jobflow'/ }
    end

    context 'when jobflow does not exist' do
      let(:configuration) { {enabled: true} }
      let(:options) { {jobflow: 'j-XYZ'} }
      before do
        mock_command(/\Aelastic-mapreduce/, mock_failure)
      end
      it { expect { subject }.to raise_error Thor::RequiredArgumentMissingError, /Value 'j-XYZ' for '--jobflow' doesn't exist/ }
    end

    context 'when jobflow exists' do
      let(:configuration) { {enabled: true} }
      let(:options) { {jobflow: 'j-XYZ'} }
      before do
        mock_command(/\Aelastic-mapreduce/, mock_success)
      end
      it do
        expect { subject }.to_not raise_error
        configuration[:jobflow].should == 'j-XYZ'
      end
    end

    context 'when jobflow is symbolic' do
      let(:configuration) { {enabled: true, jobflows: {'build' => 'j-XYZ'}} }
      let(:options) { {jobflow: 'build', } }
      before do
        mock_command(/\Aelastic-mapreduce/, mock_success)
      end
      it do
        expect { subject }.to_not raise_error
        configuration[:jobflow].should == 'j-XYZ'
      end
    end
  end
end
