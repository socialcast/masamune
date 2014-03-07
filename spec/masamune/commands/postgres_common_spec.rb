require 'spec_helper'

shared_examples_for Masamune::Commands::PostgresCommon do
  describe '#command_env' do
    subject do
      instance.command_env
    end

    context 'by default' do
      it { should == {} }
    end

    context 'with pgpass_file' do
      let(:configuration) { {:pgpass_file => 'pgpass_file'} }

      before do
        File.stub(:readable?) { true }
      end

      it { should == {'PGPASSFILE' => 'pgpass_file'} }
    end

    context 'with pgpass_file that is not readable' do
      let(:configuration) { {:pgpass_file => 'pgpass_file'} }

      before do
        File.stub(:readable?) { false }
      end

      it { should == {} }
    end
  end
end
