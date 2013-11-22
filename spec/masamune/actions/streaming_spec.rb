require 'spec_helper'

describe Masamune::Actions::Streaming do
  let(:klass) do
    Class.new do
      include Masamune::HasContext
      include Masamune::Actions::Streaming
    end
  end

  let(:instance) { klass.new }
  let(:configuration) { {} }

  before do
    instance.stub_chain(:configuration, :hadoop_streaming).and_return(configuration)
  end

  describe '.streaming' do
    before do
      mock_command(/\Ahadoop/, mock_success)
    end

    subject { instance.streaming }

    it { should be_success }
  end
end
