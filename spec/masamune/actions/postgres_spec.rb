#  The MIT License (MIT)
#
#  Copyright (c) 2014-2016, VMware, Inc. All Rights Reserved.
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in
#  all copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
#  THE SOFTWARE.

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
  let(:configuration) { { database: 'test' } }
  let(:postgres_helper) { double }
  let(:catalog) { double }

  before do
    allow(instance).to receive(:filesystem) { filesystem }
    allow(instance).to receive(:catalog) { catalog }
    allow(instance).to receive(:postgres_helper) { postgres_helper }
    allow(instance).to receive_message_chain(:configuration, :commands, :postgres).and_return(configuration)
    allow(instance).to receive_message_chain(:configuration, :with_quiet).and_yield
    allow(instance).to receive_message_chain(:define_schema, :to_file) { 'catalog.psql' }
  end

  describe '.postgres' do
    subject(:action) { instance.postgres }

    context 'when success' do
      before do
        mock_command(/\APGOPTIONS=.* psql/, mock_success)
      end

      it { is_expected.to be_success }
    end

    context 'when failure' do
      before do
        mock_command(/\APGOPTIONS=.* psql/, mock_failure)
      end

      it { is_expected.not_to be_success }
    end

    context 'with retries and backoff' do
      before do
        allow(instance).to receive_message_chain(:configuration, :commands, :postgres).and_return(retries: 1, backoff: 10)
        expect(Masamune::Commands::RetryWithBackoff).to receive(:new).with(anything, hash_including(retries: 1, backoff: 10)).once.and_call_original
        mock_command(/\APGOPTIONS=.* psql/, mock_success)
      end

      it { is_expected.to be_success }
    end
  end

  describe '.after_initialize' do
    let(:options) { { initialize: true } }
    let(:setup_files) { [] }
    let(:schema_files) { [] }
    let(:configuration) { { database: 'test', setup_files: setup_files, schema_files: schema_files } }

    subject(:after_initialize_invoke) do
      instance.after_initialize_invoke(options)
    end

    context 'without --initialize' do
      let(:options) { {} }
      before do
        expect(instance).to_not receive(:postgres_admin)
        expect(instance).to_not receive(:postgres)
        after_initialize_invoke
      end
      it 'should not call postgres_admin or postgres' do
      end
    end

    context 'when database does not exist' do
      before do
        expect(postgres_helper).to receive(:database_exists?).and_return(false)
        expect(instance).to receive(:postgres_admin).with(action: :create, database: 'test', safe: true).once
        expect(instance).to receive(:postgres).with(file: 'catalog.psql', retries: 0).once
        after_initialize_invoke
      end
      it 'should call posgres_admin once' do
      end
    end

    context 'when database exists' do
      before do
        expect(postgres_helper).to receive(:database_exists?).and_return(true)
        expect(instance).to receive(:postgres_admin).never
        expect(instance).to receive(:postgres).with(file: 'catalog.psql', retries: 0).once
        after_initialize_invoke
      end
      it 'should not call postgres_admin' do
      end
    end

    context 'when setup_files are configured' do
      let(:setup_files) { ['setup.psql'] }
      before do
        expect(postgres_helper).to receive(:database_exists?).and_return(true)
        expect(instance).to receive(:postgres).with(file: setup_files.first, retries: 0).once
        expect(instance).to receive(:postgres).with(file: 'catalog.psql', retries: 0).once
        after_initialize_invoke
      end
      it 'should call postgres with setup_files' do
      end
    end

    context 'when schema_files are configured' do
      let(:schema_files) { ['schema.psql'] }
      before do
        filesystem.touch!(*schema_files)
        expect(postgres_helper).to receive(:database_exists?).and_return(true)
        expect(instance).to receive(:postgres).once
        after_initialize_invoke
      end
      it 'should call postgres with schema_files' do
      end
    end

    context 'when schema_files that are globs are configured' do
      let(:schema_files) { ['schema*.psql'] }
      before do
        filesystem.touch!('schema_1.psql', 'schema_2.psql')
        expect(postgres_helper).to receive(:database_exists?).and_return(true)
        expect(instance).to receive(:postgres).with(file: 'catalog.psql', retries: 0).once
        after_initialize_invoke
      end
      it 'should call postgres with schema_files' do
      end
    end

    context 'when ruby schema_files configured' do
      let(:schema_files) { ['schema.rb'] }
      before do
        filesystem.touch!('schema.rb')
        expect(postgres_helper).to receive(:database_exists?).and_return(true)
        expect(instance).to receive(:postgres).with(file: 'catalog.psql', retries: 0).once
        after_initialize_invoke
      end
      it 'should call postgres with schema_files' do
      end
    end
  end
end
