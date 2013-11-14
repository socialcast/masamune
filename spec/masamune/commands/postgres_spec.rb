require 'spec_helper'

describe Masamune::Commands::Postgres do
  let(:configuration) { {:path => 'psql', :database => 'postgres', :options => command_options} }
  let(:general_options) { {} }
  let(:command_options) { [] }
  let(:context_options) { {} }

  let(:instance) { Masamune::Commands::Postgres.new(general_options.merge(context_options)) }

  before do
    instance.stub(:configuration) { configuration }
  end

  describe '#stdin' do
    context 'with exec' do
      let(:context_options) { {exec: %q(SELECT * FROM table;)} }
      subject { instance.stdin }
      it { should be_a(StringIO) }
      its(:string) { should == %q(SELECT * FROM table;) }
    end
  end

  describe '#command_args' do
    let(:default_command) { ['psql', '--dbname', 'postgres'] }

    subject do
      instance.command_args
    end

    it { should == default_command }

    context 'with command options' do
      let(:command_options) { [{'-A' => nil}] }
      it { should == [*default_command, '-A'] }
    end

    context 'with file' do
      let(:context_options) { {file: 'zomg.hql'} }
      it { should == [*default_command, '-f', 'zomg.hql'] }
    end

    context 'with variables' do
      let(:context_options) { {variables: {R: 'R2DO', C: 'C3PO'}} }
      it { should == [*default_command, '-P', 'R=R2DO', '-P', 'C=C3PO'] }
    end
  end
end
