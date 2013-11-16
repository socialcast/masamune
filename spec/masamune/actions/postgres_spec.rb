require 'spec_helper'

describe Masamune::Actions::Postgres do
  # TODO move to universal example group
  let(:client) { Masamune::Client.new }
  let(:default_configuration) { client.configuration }

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
    let(:base) { double }
    let(:configuration) { {} }

    before do
      base.stub_chain(:configuration, :postgres).and_return(default_configuration.postgres.merge(configuration))
    end

    subject(:before_initialize_invoke) do
      klass.before_initialize_invoke(base)
    end

    context 'when database does not exist' do
      before do
        base.should_receive(:postgres).with(exec: 'SELECT version();', fail_fast: false).and_return(mock_failure)
        base.should_receive(:postgres_admin).with(action: :create, database: an_instance_of(String)).once
        before_initialize_invoke
      end
      it 'should call posgres_admin once' do; end
    end

    context 'when database exists' do
      before do
        base.should_receive(:postgres).with(exec: 'SELECT version();', fail_fast: false).and_return(mock_success)
        base.should_receive(:postgres_admin).never
        before_initialize_invoke
      end
      it 'should not call postgres_admin' do; end
    end

    context 'when setup_files are configured' do
      let(:setup_file) { 'setup.psql' }
      let(:configuration) { {setup_files: [setup_file]} }
      before do
        base.should_receive(:postgres).with(exec: 'SELECT version();', fail_fast: false).and_return(mock_success)
        base.should_receive(:postgres).with(file: setup_file).once
        before_initialize_invoke
      end
      it 'should not call postgres_admin' do; end
    end

    context 'when schema_files are configured' do
      let(:schema_file) { 'schema.psql' }
      let(:configuration) { {schema_files: [schema_file]} }
      before do
        base.should_receive(:postgres).with(exec: 'SELECT version();', fail_fast: false).and_return(mock_success)
        base.should_receive(:postgres).with(file: schema_file).once
        before_initialize_invoke
      end
      it 'should not call postgres_admin' do; end
    end
  end
end
