require 'spec_helper'

describe Masamune::Commands::LineFormatter do
  let(:options) { {} }
  let(:delegate) { double }
  let(:instance) { described_class.new(delegate, options) }

  describe '#handle_stdout' do
    let(:ifs) { "\t" }
    let(:ofs) { ','  }

    let(:options) { {ifs: ifs, ofs: ofs} }

    let(:line) { %w(this is not a line) }

    before do
      delegate.should_receive(:handle_stdout).with(line.join(ofs), 0).once
      instance.handle_stdout(line.join(ifs), 0)
    end

    it 'converts ifs to ofs' do; end
  end
end
