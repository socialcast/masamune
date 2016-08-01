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

describe Masamune::Commands::PostgresAdmin do
  let(:configuration) { { create_db_path: 'createdb', drop_db_path: 'dropdb', hostname: 'localhost', username: 'postgres' } }
  let(:postgres_configuration) { {} }
  let(:attrs) { {} }

  let(:delegate) { double }
  let(:instance) { described_class.new(delegate, attrs) }

  before do
    allow(delegate).to receive_message_chain(:configuration, :commands, :postgres).and_return(postgres_configuration)
    allow(delegate).to receive_message_chain(:configuration, :commands, :postgres_admin).and_return(configuration)
  end

  describe '#command_args' do
    subject do
      instance.command_args
    end

    context 'action :create with database' do
      let(:attrs) { { action: :create, database: 'zombo' } }
      it { is_expected.to eq(['createdb', '--host=localhost', '--username=postgres', '--no-password', 'zombo']) }
    end

    context 'action :create with database with postgres database configuration (string)' do
      let(:postgres_configuration) { { 'database' => 'test' } }
      let(:attrs) { { action: :create, database: 'zombo' } }
      it { is_expected.to eq(['createdb', '--host=localhost', '--username=postgres', '--no-password', 'zombo']) }
    end

    context 'action :create with database with postgres database configuration (symbol)' do
      let(:postgres_configuration) { { database: 'test' } }
      let(:attrs) { { action: :create, database: 'zombo' } }
      it { is_expected.to eq(['createdb', '--host=localhost', '--username=postgres', '--no-password', 'zombo']) }
    end

    context 'action :create without database' do
      let(:attrs) { { action: :create } }
      it { expect { subject }.to raise_error ArgumentError, ':database must be given' }
    end

    context 'action :drop with database' do
      let(:attrs) { { action: :drop, database: 'zombo' } }
      it { is_expected.to eq(['dropdb', '--if-exists', '--host=localhost', '--username=postgres', '--no-password', 'zombo']) }
    end

    context 'action :drop with database and :output' do
      let(:attrs) { { action: :drop, database: 'zombo', output: 'zombo.csv' } }
      it { is_expected.to eq(['dropdb', '--if-exists', '--host=localhost', '--username=postgres', '--no-password', 'zombo']) }
    end

    context 'action :dump with database' do
      let(:attrs) { { action: :dump, database: 'zombo' } }
      it { is_expected.to eq(['pg_dump', '--no-owner', '--no-privileges', '--oids', '--schema=public', '--host=localhost', '--username=postgres', '--no-password', '--dbname=zombo']) }
    end

    context 'action :dump with database and :output' do
      let(:attrs) { { action: :dump, database: 'zombo', output: 'zombo.csv' } }
      it { is_expected.to eq(['pg_dump', '--no-owner', '--no-privileges', '--oids', '--schema=public', '--host=localhost', '--username=postgres', '--no-password', '--dbname=zombo', '--file=zombo.csv']) }
    end

    context 'action :drop without database' do
      let(:attrs) { { action: :drop } }
      it { expect { subject }.to raise_error ArgumentError, ':database must be given' }
    end

    context 'action unfuddle with database' do
      let(:attrs) { { action: :unfuddle, database: 'zombo' } }
      it { expect { subject }.to raise_error ArgumentError, ':action must be :create, :drop, or :dump' }
    end
  end

  it_should_behave_like Masamune::Commands::PostgresCommon
end
