require 'spec_helper'

describe Masamune::Actions::Hive do
  let(:klass) do
    Class.new do
      extend Masamune::Thor::BeforeInitializeCallbacks
      include Masamune::HasContext
      include Masamune::Actions::Hive
    end
  end

  let(:instance) { klass.new }

  before do
    instance.stub_chain(:configuration, :elastic_mapreduce).and_return({})
  end

  describe '.hive' do
    before do
      mock_command(/\Ahive/, mock_success)
    end

    subject { instance.hive }

    it { should be_success }

    context 'with jobflow' do
      before do
        instance.stub_chain(:configuration, :elastic_mapreduce).and_return({jobflow: 'j-XYZ'})
        mock_command(/\Ahive/, mock_failure)
        mock_command(/\Aelastic-mapreduce/, mock_success, StringIO.new('ssh fakehost exit'))
        mock_command(/\Assh fakehost hive/, mock_success)
      end

      subject { instance.hive }

      it { should be_success }
    end
  end
end
