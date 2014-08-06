shared_examples_for Masamune::Commands::PostgresCommon do
  describe '#command_env' do
    subject(:env) do
      instance.command_env
    end

    context 'by default' do
      it { expect(env['PGOPTIONS']).to eq('--client-min-messages=warning') }
    end

    context 'with pgpass_file' do
      let(:configuration) { {:pgpass_file => 'pgpass_file'} }

      before do
        allow(File).to receive(:readable?) { true }
      end

      it { expect(env['PGPASSFILE']).to eq('pgpass_file') }
    end

    context 'with pgpass_file that is not readable' do
      let(:configuration) { {:pgpass_file => 'pgpass_file'} }

      before do
        allow(File).to receive(:readable?) { false }
      end

      it { expect(env).to_not include 'PGPASSFILE' }
    end
  end
end
