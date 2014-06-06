require 'spec_helper'

describe Masamune do
  it { is_expected.to be_a(Module) }

  describe '#context' do
    subject { super().context }
    it { should be_a(Masamune::Context) }
  end
end
