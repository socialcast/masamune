require 'spec_helper'

describe Masamune::Commands::Hive do
  let(:configuration) { {:options => options} }
  let(:options) { [] }
  let(:attrs) { {} }

  let(:delegate) { double }
  let(:instance) { described_class.new(delegate, configuration.merge(attrs)) }

  describe '#stdin' do
    context 'with exec' do
      let(:attrs) { {exec: %q(SELECT * FROM table;)} }
      subject { instance.stdin }
      it { should be_a(StringIO) }
      its(:string) { should == %q(SELECT * FROM table;) }
    end
  end

  describe '#command_args' do
    subject do
      instance.command_args
    end

    it { should == ['hive'] }

    context 'with command attrs' do
      let(:options) { [{'-d' => 'DATABASE=development'}] }
      it { should == ['hive', '-d', 'DATABASE=development'] }
    end

    context 'with file' do
      let(:attrs) { {file: 'zomg.hql'} }
      it { should == ['hive', '-f', 'zomg.hql'] }
    end

    context 'with variables' do
      let(:attrs) { {variables: {R: 'R2DO', C: 'C3PO'}} }
      it { should == ['hive', '-d', 'R=R2DO', '-d', 'C=C3PO'] }
    end
  end
end
