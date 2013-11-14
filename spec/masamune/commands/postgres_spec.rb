require 'spec_helper'

describe Masamune::Commands::Postgres do
  let(:general_options) { {} }
  let(:command_options) { [] }
  let(:context_options) { {} }

  before do
    Masamune.configuration.postgres[:options] = command_options
  end

  let(:instance) { Masamune::Commands::Postgres.new(general_options.merge(context_options)) }

  describe '#stdin' do
    context 'with exec' do
      let(:context_options) { {exec: %q(SELECT * FROM table;)} }
      subject { instance.stdin }
      it { should be_a(StringIO) }
      its(:string) { should == %q(SELECT * FROM table;) }
    end
  end

  describe '#command_args' do
    subject do
      instance.command_args
    end

    it { should == ['psql'] }

    context 'with command options' do
      let(:command_options) { [{'-d' => 'DATABASE=development'}] }
      it { should == ['psql', '-d', 'DATABASE=development'] }
    end

    context 'with file' do
      let(:context_options) { {file: 'zomg.hql'} }
      it { should == ['psql', '-f', 'zomg.hql'] }
    end

    context 'with variables' do
      let(:context_options) { {variables: {R: 'R2DO', C: 'C3PO'}} }
      it { should == ['psql', '-P', 'R=R2DO', '-P', 'C=C3PO'] }
    end
  end
end
