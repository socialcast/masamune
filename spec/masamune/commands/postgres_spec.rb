require 'spec_helper'

describe Masamune::Commands::Postgres do
  let(:configuration) { {:path => 'psql', :database => 'postgres', :options => options} }
  let(:options) { [] }
  let(:attrs) { {} }

  let(:delegate) { double }
  let(:instance) { described_class.new(delegate, attrs) }

  before do
    delegate.stub_chain(:configuration, :postgres).and_return(configuration)
  end

  describe '#stdin' do
    context 'with input' do
      let(:attrs) { {input: %q(SELECT * FROM table;)} }
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

    context 'with options' do
      let(:options) { [{'-A' => nil}] }
      it { should == [*default_command, '-A'] }
    end

    context 'with file' do
      let(:attrs) { {file: 'zomg.hql'} }
      it { should == [*default_command, '--file=zomg.hql'] }
    end

    context 'with variables' do
      let(:attrs) { {variables: {R: 'R2DO', C: 'C3PO'}} }
      it { should == [*default_command, %q(--set=R='R2DO'), %q(--set=C='C3PO')] }
    end

    context 'with csv' do
      let(:attrs) { {csv: true} }
      it { should == [*default_command, '--no-align', '--field-separator=,', '--pset=footer'] }
    end
  end
end
