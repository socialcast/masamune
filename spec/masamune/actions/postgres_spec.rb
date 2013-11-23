require 'spec_helper'

describe Masamune::Actions::Postgres do
  let(:klass) do
    Class.new do
      extend Masamune::Thor::BeforeInitializeCallbacks
      include Masamune::HasContext
      include Masamune::Actions::Postgres
    end
  end

  let(:instance) { klass.new }
  let(:configuration) { {database: 'test'} }

  before do
    instance.stub_chain(:configuration, :postgres).and_return(configuration)
  end

  describe '.postgres' do
    before do
      mock_command(/\Apsql/, mock_success)
    end

    subject { instance.postgres }

    it { should be_success }
  end

  describe '.after_initialize' do
    let(:options) { {} }
    let(:setup_files) { [] }
    let(:schema_files) { [] }
    let(:configuration) { {database: 'test', setup_files: setup_files, schema_files: schema_files} }

    subject(:after_initialize_invoke) do
      klass.after_initialize_invoke(instance, options)
    end

    context 'when database does not exist' do
      before do
        instance.should_receive(:postgres).with(exec: 'SELECT version();', fail_fast: false).and_return(mock_failure)
        instance.should_receive(:postgres_admin).with(action: :create, database: 'test', safe: true).once
        after_initialize_invoke
      end
      it 'should call posgres_admin once' do; end
    end

    context 'when database exists' do
      before do
        instance.should_receive(:postgres).with(exec: 'SELECT version();', fail_fast: false).and_return(mock_success)
        instance.should_receive(:postgres_admin).never
        after_initialize_invoke
      end
      it 'should not call postgres_admin' do; end
    end

    context 'when setup_files are configured' do
      let(:setup_files) { ['setup.psql'] }
      before do
        instance.should_receive(:postgres).with(exec: 'SELECT version();', fail_fast: false).and_return(mock_success)
        instance.should_receive(:postgres).with(file: setup_files.first).once
        after_initialize_invoke
      end
      it 'should call postgres with setup_files' do; end
    end

    context 'when schema_files are configured' do
      let(:schema_files) { ['schema.psql'] }
      before do
        instance.should_receive(:postgres).with(exec: 'SELECT version();', fail_fast: false).and_return(mock_success)
        instance.should_receive(:postgres).with(file: schema_files.first).once
        after_initialize_invoke
      end
      it 'should call postgres with schema_files' do; end
    end

    context 'when schema_files are configured' do
      let(:schema_files) { ['schema.psql'] }
      before do
        instance.should_receive(:postgres).with(exec: 'SELECT version();', fail_fast: false).and_return(mock_success)
        instance.should_receive(:postgres).with(file: schema_files.first).once
        after_initialize_invoke
      end
      it 'should call postgres with schema_files' do; end
    end
  end
end
