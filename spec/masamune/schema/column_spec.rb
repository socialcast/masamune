require 'spec_helper'

describe Masamune::Schema::Column do
  describe '#ruby_value' do
    subject(:result) { column.ruby_value(value) }

    context 'with type :yaml and sub_type :boolean' do
      let(:column) { described_class.new(id: 'yaml', type: :yaml, sub_type: :boolean) }
      let(:value) do
        {
          'true'         => true,
          'one'          => '1',
          'zero'         => '0',
          'false'        => false,
          'string'       => 'string',
          'one_integer'  => 1,
          'zero_integer' => 0
        }.to_yaml
      end

      it 'should cast yaml to ruby' do
        expect(result['true']).to eq(true)
        expect(result['false']).to eq(false)
        expect(result['one']).to eq(true)
        expect(result['zero']).to eq(false)
        expect(result['one_integer']).to eq(true)
        expect(result['zero_integer']).to eq(false)
        expect(result.key?('string')).to eq(false)
      end
    end
  end

  describe '#==' do
    subject { column == other }

    context 'when identical reference' do
      let(:column) { described_class.new id: 'name', type: :string }
      let(:other) { column }
      it { is_expected.to eq(true) }
    end

    context 'when identical value' do
      let(:column) { described_class.new id: 'name', type: :string }
      let(:other) { column.dup }
      it { is_expected.to eq(true) }
    end

    context 'when different value' do
      let(:column) { described_class.new id: 'name', type: :string }
      let(:other) { described_class.new id: 'name', type: :integer }
      it { is_expected.to eq(false) }
    end
  end
end
