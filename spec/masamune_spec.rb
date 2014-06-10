require 'spec_helper'

describe Masamune do
  it { is_expected.to be_a(Module) }

  describe '#context' do
    subject { described_class.context }
    it { is_expected.to be_a(Masamune::Context) }
  end
end
