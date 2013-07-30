require 'spec_helper'

describe Masamune::Commands::Hive do
  let(:general_options) { {} }
  let(:command_options) { [] }
  let(:context_options) { {} }

  before do
    Masamune.configuration.hive[:options] = command_options
  end

  let(:instance) { Masamune::Commands::Hive.new(general_options.merge(context_options)) }

  describe '#stdin' do
    context 'with exec' do
      subject { instance.stdin }

      context 'with quoted sql' do
        let(:context_options) { {exec: %q('SELECT * FROM table;')} }
        it { should be_a(StringIO) }
        its(:string) { should == %q(SELECT * FROM table;) }
      end

      context 'with ; terminated sql' do
        let(:context_options) { {exec: %q(SELECT * FROM table;;)} }
        it { should be_a(StringIO) }
        its(:string) { should == %q(SELECT * FROM table;) }
      end

      context 'with multi line sql' do
        let(:context_options) do
          {
            exec: <<-EOS
              SELECT
                *
              FROM
                table
              ;

            EOS
          }
        end
        it { should be_a(StringIO) }
        its(:string) { should == %q(SELECT * FROM table;) }
      end

      context 'with un-quoted sql' do
        let(:context_options) { {exec: %q(SELECT * FROM table)} }
        it { should be_a(StringIO) }
        its(:string) { should == %q(SELECT * FROM table;) }
      end
    end
  end

  describe '#command_args' do
    subject do
      instance.command_args
    end

    it { should == ['hive'] }

    context 'with command options' do
      let(:command_options) { [{'-d' => 'DATABASE=development'}] }
      it { should == ['hive', '-d', 'DATABASE=development'] }
    end

    context 'with variables' do
      let(:context_options) { {variables: {R: 'R2DO', C: 'C3PO'}} }
      it { should == ['hive', '-d', 'R=R2DO', '-d', 'C=C3PO'] }
    end
  end
end
