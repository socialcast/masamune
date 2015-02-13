require 'spec_helper'

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
    it { expect(event.attributes[:tenant_id].type).to eq(:integer) }
    it { expect(event.attributes[:user_id].type).to eq(:integer) }
  end

  context 'with array attributes' do
    let(:event) do
      described_class.new id: 'user',
        attributes: [
          Masamune::Schema::Event::Attribute.new(id: 'group_id', type: :integer, array: true),
        ]
    end

    it { expect(event.attributes).to include :group_id }
    it { expect(event.attributes[:group_id].type).to eq(:integer) }
    it { expect(event.attributes[:group_id].array).to be(true) }
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
      expect(attribute.array).to eq(false)
    end
  end
end
