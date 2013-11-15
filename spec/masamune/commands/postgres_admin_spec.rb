require 'spec_helper'

describe Masamune::Commands::PostgresAdmin do
  let(:configuration) { {:create_db_path => 'createdb', :drop_db_path => 'dropdb', :hostname => 'localhost', :username => 'postgres'} }
  let(:general_options) { {} }
  let(:command_options) { [] }
  let(:context_options) { {} }

  let(:instance) { Masamune::Commands::PostgresAdmin.new(general_options.merge(context_options)) }

  before do
    instance.stub(:configuration) { configuration }
  end

  describe '#command_args' do
    subject do
      instance.command_args
    end

    context 'action :create with database' do
      let(:context_options) { {action: :create, database: 'zombo'} }
      it { should == ['createdb', '--host', 'localhost', '--username', 'postgres', '--no-password', 'zombo'] }
    end

    context 'action :create without database' do
      let(:context_options) { {action: :create} }
      it { expect { subject }.to raise_error ArgumentError, ':database must be given' }
    end

    context 'action :drop with database' do
      let(:context_options) { {action: :drop, database: 'zombo'} }
      it { should == ['dropdb', '--host', 'localhost', '--username', 'postgres', '--no-password', 'zombo'] }
    end

    context 'action :drop without database' do
      let(:context_options) { {action: :drop} }
      it { expect { subject }.to raise_error ArgumentError, ':database must be given' }
    end

    context 'action unfuddle with database' do
      let(:context_options) { {action: :unfuddle, database: 'zombo'} }
      it { expect { subject }.to raise_error ArgumentError, ':action must be :create or :drop' }
    end
  end
end
