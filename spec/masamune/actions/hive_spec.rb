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
  let(:configuration) { { database: 'test' } }

  before do
    filesystem.add_path(:tmp_dir, File.join(Dir.tmpdir, SecureRandom.hex))
    allow(instance).to receive(:filesystem) { filesystem }
    allow(instance).to receive_message_chain(:configuration, :commands, :hive).and_return(configuration)
    allow(instance).to receive_message_chain(:configuration, :commands, :aws_emr).and_return({})
    allow(instance).to receive_message_chain(:define_schema, :to_file) { 'schema.hql' }
    allow_any_instance_of(Masamune::MockFilesystem).to receive(:copy_file_to_dir)
  end

  describe '.hive' do
    subject(:action) { instance.hive }

    context 'when success' do
      before do
        mock_command(/\Ahive/, mock_success)
      end

      it { is_expected.to be_success }
    end

    context 'when failure' do
      before do
        mock_command(/\Ahive/, mock_failure)
      end

      it { is_expected.not_to be_success }
    end

    context 'with cluster_id' do
      before do
        allow(instance).to receive_message_chain(:configuration, :commands, :aws_emr).and_return(cluster_id: 'j-XYZ')
        mock_command(/\Aaws emr/, mock_success, StringIO.new('ssh fakehost exit'))
        mock_command(/\Assh fakehost hive/, mock_success)
      end

      subject { instance.hive }

      it { is_expected.to be_success }
    end

    context 'with retries and backoff' do
      before do
        allow(instance).to receive_message_chain(:configuration, :commands, :hive).and_return(retries: 1, backoff: 10)
        expect(Masamune::Commands::RetryWithBackoff).to receive(:new).with(anything, hash_including(retries: 1, backoff: 10)).once.and_call_original
        mock_command(/\Ahive/, mock_success)
      end

      it { is_expected.to be_success }
    end
  end

  describe '.after_initialize' do
    let(:options) { { initialize: true } }
    let(:configuration) { { database: 'test' } }

    subject(:after_initialize_invoke) do
      instance.after_initialize_invoke(options)
    end

    context 'without --initialize' do
      let(:options) { {} }
      before do
        expect(instance).to_not receive(:hive)
        after_initialize_invoke
      end
      it 'should not call hive' do
      end
    end

    context 'with default database' do
      let(:configuration) { { database: 'default' } }
      before do
        expect(instance).to receive(:hive).with(exec: an_instance_of(String)).never
        after_initialize_invoke
      end
      it 'should not call hive with create database' do
      end
    end

    context 'with database' do
      before do
        expect(instance).to receive(:hive).with(exec: 'CREATE DATABASE IF NOT EXISTS test;', database: nil).once.and_return(mock_success)
        expect(instance).to receive(:hive).with(file: an_instance_of(String)).once.and_return(mock_success)
        after_initialize_invoke
      end
      it 'should call hive with create database' do
      end
    end

    context 'with location' do
      let(:configuration) { { database: 'test', location: '/tmp' } }
      before do
        expect(instance).to receive(:hive).with(exec: 'CREATE DATABASE IF NOT EXISTS test LOCATION "/tmp";', database: nil).once.and_return(mock_success)
        expect(instance).to receive(:hive).with(file: an_instance_of(String)).once.and_return(mock_success)
        after_initialize_invoke
      end
      it 'should call hive with create database' do
      end
    end

    context 'with dry_run' do
      let(:options) { { initialize: true, dry_run: true } }
      before do
        expect(instance).to receive(:hive).with(exec: 'CREATE DATABASE IF NOT EXISTS test;', database: nil).once.and_return(mock_success)
        expect(instance).to receive(:hive).with(file: an_instance_of(String)).once.and_return(mock_success)
        expect(instance).to receive(:hive).with(exec: 'SHOW TABLES;', safe: true, fail_fast: false).once.and_return(mock_success)
        after_initialize_invoke
      end
      it 'should call hive with show tables' do
      end
    end
  end
end
