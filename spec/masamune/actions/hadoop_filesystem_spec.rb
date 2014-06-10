require 'spec_helper'

describe Masamune::Actions::HadoopFilesystem do
  let(:klass) do
    Class.new do
      include Masamune::HasContext
      include Masamune::Actions::HadoopFilesystem
    end
  end

  let(:instance) { klass.new }

  describe '.hadoop_filesystem' do
    before do
      mock_command(/\Ahadoop/, mock_success)
    end

    subject { instance.hadoop_filesystem }

    it { is_expected.to be_success }
  end
end
