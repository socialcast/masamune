require 'spec_helper'

describe Masamune::Actions::HadoopFilesystem do
  let(:klass) do
    Class.new do
      include Masamune::HasContext
      include Masamune::Actions::HadoopFilesystem
    end
  end

  let(:instance) { klass.new }
  let(:configuration) { {} }

  before do
    instance.stub_chain(:configuration, :hadoop_filesystem).and_return(configuration)
  end

  describe '.hadoop_filesystem' do
    before do
      mock_command(/\Ahadoop/, mock_success)
    end

    subject { instance.hadoop_filesystem }

    it { should be_success }
  end
end
