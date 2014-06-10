shared_examples_for Masamune::Commands::PostgresCommon do
  describe '#command_env' do
    subject do
      instance.command_env
    end

    context 'by default' do
      it { is_expected.to eq({}) }
    end

    context 'with pgpass_file' do
      let(:configuration) { {:pgpass_file => 'pgpass_file'} }

      before do
        allow(File).to receive(:readable?) { true }
      end

      it { is_expected.to eq({'PGPASSFILE' => 'pgpass_file'}) }
    end

    context 'with pgpass_file that is not readable' do
      let(:configuration) { {:pgpass_file => 'pgpass_file'} }

      before do
        allow(File).to receive(:readable?) { false }
      end

      it { is_expected.to eq({}) }
    end
  end
end
