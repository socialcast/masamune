require 'spec_helper'

describe Masamune::Schema::File do
  context 'without id' do
    subject(:file) { described_class.new }
    it { expect { file }.to raise_error ArgumentError }
  end
end
