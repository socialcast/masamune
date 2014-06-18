require 'spec_helper'

describe Masamune do
  it { is_expected.to be_a(Module) }

  describe '#environment' do
    subject { described_class.environment }
    it { is_expected.to be_a(Masamune::Environment) }
  end
end
