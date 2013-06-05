require 'spec_helper'

describe Masamune::Commands::Hive do
  let(:general_options) { {} }
  let(:command_options) { [] }
  let(:context_options) { {} }

  before do
    Masamune.configuration.add_command_options(:hive) do
      command_options
    end
  end

  let(:instance) { Masamune::Commands::Hive.new(general_options.merge(context_options)) }

  describe '#command_args' do
    context 'with exec' do
      subject { instance.command_args }

      context 'with quoted sql' do
        let(:context_options) { {exec: %q('SELECT * FROM table;')} }
        it { should == ['hive', '-e', %q('SELECT * FROM table;')] }
      end

      context 'with un-quoted sql' do
        let(:context_options) { {exec: %q(SELECT * FROM table)} }
        it { should == ['hive', '-e', %q(SELECT * FROM table)] }
      end
    end

    context 'with exec and quote' do
      subject { instance.command_args }

      context 'with quoted sql' do
        let(:context_options) { {exec: %q('SELECT * FROM table;'), quote: true} }
        it { should == ['hive', '-e', %q('SELECT * FROM table;')] }
      end

      context 'with un-quoted sql' do
        let(:context_options) { {exec: %q(SELECT * FROM table), quote: true} }
        it { should == ['hive', '-e', %q('SELECT * FROM table;')] }
      end
    end
  end
end
