require 'spec_helper'

describe Masamune::Actions::Postgres do
  let(:klass) do
    Class.new do
      extend Masamune::Thor::BeforeInitializeCallbacks
      include Masamune::Actions::Postgres
    end
  end

  let(:instance) { klass.new }

  describe '.postgres' do
    before do
      mock_command(/\Apsql/, mock_success)
    end

    subject { instance.postgres }

    it { should be_success }
  end

  describe '.before_initialize' do
    context 'when database does not exist' do
      before do
        instance.should_receive(:postgres).with(exec: 'SELECT version();', fail_fast: false).and_return(mock_failure)
        instance.should_receive(:postgres_admin).with(action: :create, database: an_instance_of(String)).once
        klass.before_initialize_invoke(instance)
      end
      it 'should call posgres_admin once' do; end
    end

    context 'when database exists' do
      before do
        instance.should_receive(:postgres).with(exec: 'SELECT version();', fail_fast: false).and_return(mock_success)
        instance.should_receive(:postgres_admin).never
        klass.before_initialize_invoke(instance)
      end
      it 'should not call postgres_admin' do; end
    end
  end
end
