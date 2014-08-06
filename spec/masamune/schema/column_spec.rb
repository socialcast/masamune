require 'spec_helper'

describe Masamune::Schema::Column do
  describe '#ruby_value' do
    subject(:result) { column.ruby_value(value) }

    context 'with :yaml type' do
      let(:column) { described_class.new(name: 'yaml', type: :yaml) }
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
        expect(result['string']).to eq('string')
        expect(result['one_integer']).to eq(1)
        expect(result['zero_integer']).to eq(0)
      end
    end
  end
end
