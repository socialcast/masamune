#  The MIT License (MIT)
#
#  Copyright (c) 2014-2016, VMware, Inc. All Rights Reserved.
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in
#  all copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
#  THE SOFTWARE.

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
    it { expect(dimension.grain).to eq(:daily) }
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
    it { expect(dimension.grain).to eq(:hourly) }
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
    it { expect(dimension.grain).to eq(:hourly) }
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

    it { expect { dimension }.to raise_error /contains undefined columns/ }
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
    it { expect(dimension.grain).to eq(:hourly) }

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

  context 'dimension with daily grain' do
    let(:dimension) { described_class.new id: 'users', type: :one, grain: :daily }

    it { expect(dimension.name).to eq('users_dimension') }
    it { expect(dimension.type).to eq(:one) }
    it { expect(dimension.grain).to eq(:daily) }
  end

  context 'dimension with unknown grain' do
    subject(:dimension) do
      described_class.new id: 'users', grain: :quarterly
    end

    it { expect { dimension }.to raise_error ArgumentError, "unknown grain 'quarterly'" }
  end
end
