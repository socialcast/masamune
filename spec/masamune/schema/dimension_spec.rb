require 'spec_helper'

describe Masamune::Schema::Dimension do
  let(:store) { double }

  context 'for type :date' do
    let(:dimension) do
      described_class.new id: 'date', type: :date,
        columns: [
          Masamune::Schema::Column.new(id: 'date_id')
        ]
    end

    it { expect(dimension.name).to eq('date_dimension') }
    it { expect(dimension.type).to eq(:date) }
  end

  context 'for type :one' do
    let(:dimension) do
      described_class.new id: 'user', type: :one,
        columns: [
          Masamune::Schema::Column.new(id: 'tenant_id'),
          Masamune::Schema::Column.new(id: 'user_id')
        ]
    end

    it { expect(dimension.name).to eq('user_dimension') }
    it { expect(dimension.type).to eq(:one) }
  end

  context 'for type :two' do
    let(:dimension) do
      described_class.new id: 'user', type: :two,
        columns: [
          Masamune::Schema::Column.new(id: 'tenant_id', index: true, natural_key: true),
          Masamune::Schema::Column.new(id: 'user_id', index: true, natural_key: true)
        ]
    end

    it { expect(dimension.name).to eq('user_dimension') }
    it { expect(dimension.type).to eq(:two) }
  end

  context 'with invalid values' do
    let(:dimension) do
      described_class.new id: 'user_account_state', type: :mini,
        columns: [
          Masamune::Schema::Column.new(id: 'name', type: :string, unique: true),
          Masamune::Schema::Column.new(id: 'description', type: :string)
        ],
        rows: [
          Masamune::Schema::Row.new(values: {
            name: 'active',
            description: 'Active',
            missing_column: true
          })
        ]
    end

    it { expect { dimension }.to raise_error ArgumentError, /contains undefined columns/ }
  end

  context 'for type :four' do
    let(:mini_dimension) do
      described_class.new id: 'user_account_state', type: :mini,
        columns: [
          Masamune::Schema::Column.new(id: 'name', type: :string, unique: true),
          Masamune::Schema::Column.new(id: 'description', type: :string)
        ],
        rows: [
          Masamune::Schema::Row.new(values: {
            name: 'active',
            description: 'Active',
          }, default: true)
        ]
    end

    let(:dimension) do
      described_class.new id: 'user', store: store, type: :four, references: [Masamune::Schema::TableReference.new(mini_dimension)],
        columns: [
          Masamune::Schema::Column.new(id: 'tenant_id', index: true, natural_key: true),
          Masamune::Schema::Column.new(id: 'user_id', index: true, natural_key: true),
          Masamune::Schema::Column.new(id: 'preferences', type: :key_value, null: true)
        ]
    end

    it { expect(dimension.name).to eq('user_dimension') }
    it { expect(dimension.type).to eq(:four) }

    describe '#stage_table' do
      let!(:stage_table) { dimension.stage_table }

      it 'should inherit id' do
        expect(stage_table.id).to eq(:user)
        expect(stage_table.name).to eq('user_dimension_stage')
      end

      it 'should inherit store' do
        expect(stage_table.store).to eq(store)
      end

      it 'should duplicate columns' do
        expect(dimension.parent).to be_nil
        expect(dimension.columns[:tenant_id].parent).to eq(dimension)
        expect(stage_table.parent).to eq(dimension)
        expect(stage_table.columns[:tenant_id].parent).to eq(stage_table)
      end

      it 'should inherit reserved_columns' do
        expect(dimension.reserved_columns.keys).to_not be_empty
        expect(stage_table.reserved_columns.keys).to eq(dimension.reserved_columns.keys)
      end
    end
  end
end
