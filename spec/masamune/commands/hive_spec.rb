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

      context 'with un-quoted sql' do
        let(:context_options) { {exec: %q(SELECT * FROM table)} }
        it { should be_a(StringIO) }
        its(:string) { should == %q(SELECT * FROM table;) }
      end
    end
  end
end
