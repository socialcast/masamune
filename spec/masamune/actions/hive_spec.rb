require 'spec_helper'

describe Masamune::Actions::Hive do
  let(:klass) do
    Class.new do
      extend Masamune::Thor::BeforeInitializeCallbacks
      include Masamune::ClientBehavior
      include Masamune::Actions::Hive
    end
  end

  let(:instance) { klass.new }
  let(:configuration) { {database: 'test'} }

  before do
    instance.stub(:configuration).and_return({hive: configuration})
  end

  describe '.hive' do
    before do
      mock_command(/\Ahive/, mock_success)
    end

    subject { instance.hive }

    it { should be_success }
  end
end
