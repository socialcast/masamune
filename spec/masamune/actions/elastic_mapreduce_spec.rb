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

  before do
    instance.stub_chain(:configuration, :elastic_mapreduce).and_return(configuration)
  end

  describe '.elasitc_mapreduce' do
    before do
      mock_command(/\Aelastic-mapreduce/, mock_success)
    end

    subject { instance.elastic_mapreduce }

    it { should be_success }
  end
end
