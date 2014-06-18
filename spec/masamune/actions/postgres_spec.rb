require 'spec_helper'

describe Masamune::Actions::Postgres do
  let(:filesystem) { Masamune::MockFilesystem.new }

  let(:klass) do
    Class.new do
      include Masamune::HasEnvironment
      include Masamune::AfterInitializeCallbacks
      include Masamune::Actions::Postgres
    end
  end

  let(:instance) { klass.new }
  let(:configuration) { {database: 'test'} }
  let(:postgres_helper) { double }

  before do
    allow(instance).to receive(:filesystem) { filesystem }
    allow(instance).to receive(:postgres_helper) { postgres_helper }
    allow(instance).to receive_message_chain(:configuration, :postgres).and_return(configuration)
    allow(instance).to receive_message_chain(:configuration, :with_quiet).and_yield
  end

  describe '.postgres' do
    before do
      mock_command(/\Apsql/, mock_success)
    end

    subject { instance.postgres }

    it { is_expected.to be_success }
  end

  describe '.after_initialize' do
    let(:options) { {} }
    let(:setup_files) { [] }
    let(:schema_files) { [] }
    let(:configuration) { {database: 'test', setup_files: setup_files, schema_files: schema_files} }

    subject(:after_initialize_invoke) do
      instance.after_initialize_invoke(options)
    end

    context 'when database does not exist' do
      before do
        expect(postgres_helper).to receive(:database_exists?).and_return(false)
        expect(instance).to receive(:postgres_admin).with(action: :create, database: 'test', safe: true).once
        after_initialize_invoke
      end
      it 'should call posgres_admin once' do; end
    end

    context 'when database exists' do
      before do
        expect(postgres_helper).to receive(:database_exists?).and_return(true)
        expect(instance).to receive(:postgres_admin).never
        after_initialize_invoke
      end
      it 'should not call postgres_admin' do; end
    end

    context 'when setup_files are configured' do
      let(:setup_files) { ['setup.psql'] }
      before do
        expect(postgres_helper).to receive(:database_exists?).and_return(true)
        expect(instance).to receive(:postgres).with(file: setup_files.first).once
        after_initialize_invoke
      end
      it 'should call postgres with setup_files' do; end
    end

    context 'when schema_files are configured' do
      let(:schema_files) { ['schema.psql'] }
      before do
        filesystem.touch!(*schema_files)
        expect(postgres_helper).to receive(:database_exists?).and_return(true)
        expect(instance).to receive(:postgres).with(file: schema_files.first).once
        after_initialize_invoke
      end
      it 'should call postgres with schema_files' do; end
    end

    context 'when schema_files that are globs are configured' do
      let(:schema_files) { ['schema*.psql'] }
      before do
        filesystem.touch!('schema_1.psql', 'schema_2.psql')
        expect(postgres_helper).to receive(:database_exists?).and_return(true)
        expect(instance).to receive(:postgres).with(file: 'schema_1.psql').once
        expect(instance).to receive(:postgres).with(file: 'schema_2.psql').once
        after_initialize_invoke
      end
      it 'should call postgres with schema_files' do; end
    end
  end
end
