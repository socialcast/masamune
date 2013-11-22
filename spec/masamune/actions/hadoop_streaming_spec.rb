require 'spec_helper'

describe Masamune::Actions::HadoopStreaming do
  let(:klass) do
    Class.new do
      include Masamune::HasContext
      include Masamune::Actions::HadoopStreaming
    end
  end

  let(:instance) { klass.new }
  let(:configuration) { {} }

  before do
    instance.stub_chain(:configuration, :hadoop_streaming).and_return(configuration)
    instance.stub_chain(:configuration, :elastic_mapreduce).and_return({})
  end

  describe '.hadoop_streaming' do
    before do
      mock_command(/\Ahadoop/, mock_success)
    end

    subject { instance.hadoop_streaming }

    it { should be_success }
  end
end
