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
    instance.stub_chain(:configuration, :elastic_mapreduce).and_return({})
    instance.stub_chain(:configuration, :hive).and_return(configuration)
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

  describe '.after_initialize' do
    let(:options) { {} }
    let(:setup_files) { [] }
    let(:schema_files) { [] }
    let(:configuration) { {database: 'test', setup_files: setup_files, schema_files: schema_files} }

    subject(:after_initialize_invoke) do
      klass.after_initialize_invoke(instance, options)
    end

    context 'when setup_files are configured' do
      let(:setup_files) { ['setup.hql'] }
      before do
        instance.should_receive(:hive).with(file: setup_files.first).once
        after_initialize_invoke
      end
      it 'should call hive with setup_file' do; end
    end

    context 'when schema_files are configured' do
      let(:schema_files) { ['schema.hql'] }
      before do
        instance.should_receive(:hive).with(file: schema_files.first).once
        after_initialize_invoke
      end
      it 'should call hive with schema_file' do; end
    end

    context 'with dryrun' do
      let(:options) { {dry_run: true} }
      before do
        instance.should_receive(:hive).with(exec: 'SHOW TABLES;', safe: true, fail_fast: false).once.and_return(mock_success)
        after_initialize_invoke
      end
      it 'should not call hive with setup_file nor schema_file' do; end
    end
  end
end
