require 'spec_helper'

describe Masamune::Commands::Hive do
  let(:configuration) { {:options => options} }
  let(:options) { [] }
  let(:attrs) { {} }

  let(:delegate) { double }
  let(:instance) { described_class.new(delegate, attrs) }

  before do
    delegate.stub_chain(:configuration, :hive).and_return(configuration)
  end

  describe '#stdin' do
    context 'with exec' do
      let(:attrs) { {exec: %q(SELECT * FROM table;)} }
      subject { instance.stdin }
      it { should be_a(StringIO) }
      its(:string) { should == %q(SELECT * FROM table;) }
    end
  end

  describe '#command_args' do
    let(:default_command) { ['hive', '--database', 'default'] }

    subject do
      instance.command_args
    end

    it { should == default_command }

    context 'with command attrs' do
      let(:options) { [{'-d' => 'DATABASE=development'}] }
      it { should == [*default_command, '-d', 'DATABASE=development'] }
    end

    context 'with file' do
      let(:attrs) { {file: 'zomg.hql'} }
      it { should == [*default_command, '-f', 'zomg.hql'] }
    end

    context 'with variables' do
      let(:attrs) { {variables: {R: 'R2DO', C: 'C3PO'}} }
      it { should == [*default_command, '-d', 'R=R2DO', '-d', 'C=C3PO'] }
    end

    context 'with setup files' do
      let(:attrs) { {setup_files: ['setup_a.hql', 'setup_b.hql']} }
      it { should == [*default_command, '-i', 'setup_a.hql', '-i', 'setup_b.hql'] }
    end

    context 'with schema files' do
      let(:attrs) { {schema_files: ['schema_a.hql', 'schema_b.hql']} }
      it { should == [*default_command, '-i', 'schema_a.hql', '-i', 'schema_b.hql'] }
    end
  end
end
