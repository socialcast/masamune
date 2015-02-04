require 'spec_helper'

describe Masamune::Actions::Hive do
  let(:klass) do
    Class.new do
      include Masamune::HasEnvironment
      include Masamune::AfterInitializeCallbacks
      include Masamune::Actions::Hive
    end
  end

  let(:filesystem) { Masamune::MockFilesystem.new }
  let(:instance) { klass.new }
  let(:configuration) { {database: 'test'} }

  before do
    filesystem.add_path(:tmp_dir, File.join(Dir.tmpdir, SecureRandom.hex))
    allow(instance).to receive(:filesystem) { filesystem }
    allow(instance).to receive_message_chain(:configuration, :elastic_mapreduce).and_return({})
    allow(instance).to receive_message_chain(:configuration, :hive).and_return(configuration)
    allow(instance).to receive_message_chain(:define_schema, :to_file) { 'schema.hql' }
    allow_any_instance_of(Masamune::MockFilesystem).to receive(:copy_file_to_dir)
  end

  describe '.hive' do
    before do
      mock_command(/\Ahive/, mock_success)
    end

    subject { instance.hive }

    it { is_expected.to be_success }

    context 'with jobflow' do
      before do
        allow(instance).to receive_message_chain(:configuration, :elastic_mapreduce).and_return({jobflow: 'j-XYZ'})
        mock_command(/\Ahive/, mock_failure)
        mock_command(/\Aelastic-mapreduce/, mock_success, StringIO.new('ssh fakehost exit'))
        mock_command(/\Assh fakehost hive/, mock_success)
      end

      subject { instance.hive }

      it { is_expected.to be_success }
    end
  end

  describe '.after_initialize' do
    let(:options) { {} }
    let(:configuration) { {database: 'test'} }

    subject(:after_initialize_invoke) do
      instance.after_initialize_invoke(options)
    end

    context 'with default database' do
      let(:configuration) { {database: 'default'} }
      before do
        expect(instance).to receive(:hive).with(exec: an_instance_of(String)).never
        after_initialize_invoke
      end
      it 'should not call hive with create database' do; end
    end

    context 'with database' do
      before do
        expect(instance).to receive(:hive).with(exec: 'CREATE DATABASE IF NOT EXISTS test;', :database => nil).once.and_return(mock_success)
        expect(instance).to receive(:hive).with(file: an_instance_of(String)).once.and_return(mock_success)
        after_initialize_invoke
      end
      it 'should call hive with create database' do; end
    end

    context 'with location' do
      let(:configuration) { {database: 'test', location: '/tmp'} }
      before do
        expect(instance).to receive(:hive).with(exec: 'CREATE DATABASE IF NOT EXISTS test LOCATION "/tmp";', :database => nil).once.and_return(mock_success)
        expect(instance).to receive(:hive).with(file: an_instance_of(String)).once.and_return(mock_success)
        after_initialize_invoke
      end
      it 'should call hive with create database' do; end
    end

    context 'with dryrun' do
      let(:options) { {dry_run: true} }
      before do
        expect(instance).to receive(:hive).with(exec: 'CREATE DATABASE IF NOT EXISTS test;', :database => nil).once.and_return(mock_success)
        expect(instance).to receive(:hive).with(exec: 'SHOW TABLES;', safe: true, fail_fast: false).once.and_return(mock_success)
        expect(instance).to receive(:hive).with(file: an_instance_of(String)).once.and_return(mock_success)
        after_initialize_invoke
      end
      it 'should call hive with show tables' do; end
    end
  end
end
