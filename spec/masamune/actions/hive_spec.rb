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
  let(:configuration) { {database: 'test'} }

  before do
    instance.stub_chain(:configuration, :hive).and_return(configuration)
    instance.stub_chain(:configuration, :elastic_mapreduce).and_return({})
  end

  describe '.hive' do
    before do
      mock_command(/\Ahive/, mock_success)
    end

    subject { instance.hive }

    it { should be_success }
=begin
    context 'with jobflow' do
      before do
        instance.stub_chain(:configuration, :elastic_mapreduce).and_return({jobflow: 'j-XYZ'})
        # mock_command(/\Aelastic_mapreduce/, mock_success)
      end

      subject { instance.hive }

      it { should be_success }
    end
=end
  end
end
