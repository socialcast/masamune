require 'spec_helper'
require 'active_support/core_ext/string/strip'

describe Masamune::Schema::Event do
  context 'without id' do
    subject(:event) { described_class.new }
    it { expect { event }.to raise_error ArgumentError }
  end

  context 'with attributes' do
    let(:event) do
      described_class.new id: 'user',
        attributes: [
          Masamune::Schema::Event::Attribute.new(id: 'tenant_id', type: :integer),
          Masamune::Schema::Event::Attribute.new(id: 'user_id', type: :integer)
        ]
    end

    it { expect(event.attributes).to include :tenant_id }
    it { expect(event.attributes).to include :user_id }
  end

  describe Masamune::Schema::Event::Attribute do
    context 'without id' do
      subject(:attribute) { described_class.new }
      it { expect { attribute }.to raise_error ArgumentError }
    end

    subject(:attribute) { described_class.new id: 'id' }

    it do
      expect(attribute.id).to eq(:id)
      expect(attribute.type).to eq(:integer)
      expect(attribute.immutable).to eq(false)
    end
  end
end
