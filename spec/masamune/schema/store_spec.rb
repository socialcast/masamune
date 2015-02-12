require 'spec_helper'

describe Masamune::Schema::Store do
  context 'without type' do
    subject(:store) { described_class.new }
    it { expect { store }.to raise_error ArgumentError, 'required parameter type: missing' }
  end

  context 'with type :unknown' do
    subject(:store) { described_class.new(type: :unknown) }
    it { expect { store }.to raise_error ArgumentError, "unknown type: 'unknown'" }
  end

  context 'with type :postgres' do
    subject(:store) { described_class.new(type: :postgres) }
    it { expect(store.format).to eq(:csv) }
    it { expect(store.headers).to be_truthy }
  end

  context 'with type :hive' do
    subject(:store) { described_class.new(type: :hive) }
    it { expect(store.format).to eq(:tsv) }
    it { expect(store.headers).to be_falsey }
  end
end
