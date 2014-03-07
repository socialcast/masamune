require 'spec_helper'
require 'masamune/commands/postgres_common_spec'

describe Masamune::Commands::PostgresAdmin do
  let(:configuration) { {:create_db_path => 'createdb', :drop_db_path => 'dropdb', :hostname => 'localhost', :username => 'postgres'} }
  let(:attrs) { {} }

  let(:delegate) { double }
  let(:instance) { described_class.new(delegate, attrs) }

  before do
    delegate.stub_chain(:configuration, :postgres).and_return({})
    delegate.stub_chain(:configuration, :postgres_admin).and_return(configuration)
  end

  describe '#command_args' do
    subject do
      instance.command_args
    end

    context 'action :create with database' do
      let(:attrs) { {action: :create, database: 'zombo'} }
      it { should == ['createdb', '--host=localhost', '--username=postgres', '--no-password', 'zombo'] }
    end

    context 'action :create without database' do
      let(:attrs) { {action: :create} }
      it { expect { subject }.to raise_error ArgumentError, ':database must be given' }
    end

    context 'action :drop with database' do
      let(:attrs) { {action: :drop, database: 'zombo'} }
      it { should == ['dropdb', '--if-exists', '--host=localhost', '--username=postgres', '--no-password', 'zombo'] }
    end

    context 'action :drop without database' do
      let(:attrs) { {action: :drop} }
      it { expect { subject }.to raise_error ArgumentError, ':database must be given' }
    end

    context 'action unfuddle with database' do
      let(:attrs) { {action: :unfuddle, database: 'zombo'} }
      it { expect { subject }.to raise_error ArgumentError, ':action must be :create or :drop' }
    end
  end

  it_should_behave_like Masamune::Commands::PostgresCommon
end
