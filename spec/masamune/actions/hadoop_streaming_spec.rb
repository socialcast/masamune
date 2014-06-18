require 'spec_helper'

describe Masamune::Actions::HadoopStreaming do
  let(:klass) do
    Class.new do
      include Masamune::HasContext
      include Masamune::Actions::HadoopStreaming
    end
  end

  let(:extra) { [] }
  let(:instance) { klass.new }

  describe '.hadoop_streaming' do
    before do
      mock_command(/\Ahadoop/, mock_success)
    end

    subject { instance.hadoop_streaming(extra: extra) }

    it { is_expected.to be_success }

    context 'with jobflow' do
      before do
        allow(instance).to receive_message_chain(:configuration, :elastic_mapreduce).and_return({jobflow: 'j-XYZ'})
        mock_command(/\Ahadoop/, mock_failure)
        mock_command(/\Aelastic-mapreduce/, mock_success, StringIO.new('ssh fakehost exit'))
        mock_command(/\Assh fakehost hadoop/, mock_success)
      end

      it { is_expected.to be_success }
    end

    context 'with jobflow and extra' do
      let(:extra) { ['-D', 'EXTRA'] }

      before do
        allow(instance).to receive_message_chain(:configuration, :elastic_mapreduce).and_return({jobflow: 'j-XYZ'})
        mock_command(/\Ahadoop/, mock_failure)
        mock_command(/\Aelastic-mapreduce/, mock_success, StringIO.new('ssh fakehost exit'))
        mock_command(/\Assh fakehost -D EXTRA hadoop/, mock_failure)
        mock_command(/\Assh fakehost hadoop .*? -D EXTRA/, mock_success)
      end

      it { is_expected.to be_success }
    end
  end
end
