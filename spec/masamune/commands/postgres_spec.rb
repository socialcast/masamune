require 'spec_helper'

describe Masamune::Commands::Postgres do
  let(:configuration) { {:path => 'psql', :database => 'postgres', :extra => extra} }
  let(:extra) { [] }
  let(:options) { {} }

  let(:instance) { Masamune::Commands::Postgres.new(configuration.merge(options)) }

  describe '#stdin' do
    context 'with input' do
      let(:options) { {input: %q(SELECT * FROM table;)} }
      subject { instance.stdin }
      it { should be_a(StringIO) }
      its(:string) { should == %q(SELECT * FROM table;) }
    end
  end

  describe '#command_args' do
    let(:default_command) { ['psql', '--host=localhost', '--dbname=postgres', '--username=postgres', '--no-password'] }

    subject do
      instance.command_args
    end

    it { should == default_command }

    context 'with command options' do
      let(:extra) { [{'-A' => nil}] }
      it { should == [*default_command, '-A'] }
    end

    context 'with file' do
      let(:options) { {file: 'zomg.hql'} }
      it { should == [*default_command, '--file=zomg.hql'] }
    end

    context 'with variables' do
      let(:options) { {variables: {R: 'R2DO', C: 'C3PO'}} }
      it { should == [*default_command, %q(--set=R='R2DO'), %q(--set=C='C3PO')] }
    end
  end
end
