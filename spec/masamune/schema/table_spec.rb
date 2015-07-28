#  The MIT License (MIT)
#
#  Copyright (c) 2014-2015, VMware, Inc. All Rights Reserved.
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

require 'spec_helper'

describe Masamune::Schema::Table do
  context 'without id' do
    subject(:table) { described_class.new }
    it { expect { table }.to raise_error ArgumentError }
  end

  context 'with name' do
    let(:table) do
      described_class.new id: 'user', name: 'account_table'
    end

    it { expect(table.name).to eq('account_table') }
  end

  context 'with format' do
    let(:table) do
      described_class.new id: 'user', properties: { format: :tsv }
    end

    it { expect(table.properties[:format]).to eq(:tsv) }
  end

  context 'with columns' do
    let(:table) do
      described_class.new id: 'user',
        columns: [
          Masamune::Schema::Column.new(id: 'tenant_id'),
          Masamune::Schema::Column.new(id: 'user_id')
        ]
    end

    it { expect(table.name).to eq('user_table') }
  end

  context 'with index columns' do
    let(:table) do
      described_class.new id: 'user',
        columns: [
          Masamune::Schema::Column.new(id: 'tenant_id', index: true),
          Masamune::Schema::Column.new(id: 'user_id', index: true)
        ]
    end

    it { expect(table.name).to eq('user_table') }
  end

  context 'with multiple index columns' do
    let(:table) do
      described_class.new id: 'user',
        columns: [
          Masamune::Schema::Column.new(id: 'tenant_id', index: ['tenant_id', 'shared']),
          Masamune::Schema::Column.new(id: 'user_id', index: ['user_id', 'shared'])
        ]
    end

    it { expect(table.name).to eq('user_table') }
  end

  context 'with multiple unique columns' do
    let(:table) do
      described_class.new id: 'user',
        columns: [
          Masamune::Schema::Column.new(id: 'tenant_id', unique: ['shared']),
          Masamune::Schema::Column.new(id: 'user_id', unique: ['user_id', 'shared'])
        ]
    end

    it { expect(table.name).to eq('user_table') }
    it { expect(table.unique_columns).to include :tenant_id }
    it { expect(table.unique_columns).to include :user_id }
    it { expect(table.stage_table.unique_columns).to be_empty }
  end

  context 'with enum column' do
    let(:table) do
      described_class.new id: 'user',
        columns: [
          Masamune::Schema::Column.new(id: 'tenant_id'),
          Masamune::Schema::Column.new(id: 'user_id'),
          Masamune::Schema::Column.new(id: 'state', type: :enum, sub_type: :user_state, values: %w(active inactive terminated), default: 'active')
        ]
    end

    it { expect(table.name).to eq('user_table') }
    it { expect(table.stage_table.name).to eq('user_table_stage') }
  end

  context 'with surrogate_key columns override' do
    let(:table) do
      described_class.new id: 'user',
        columns: [
          Masamune::Schema::Column.new(id: 'identifier', type: :uuid, surrogate_key: true),
          Masamune::Schema::Column.new(id: 'name', type: :string)
        ]
    end

    it { expect(table.name).to eq('user_table') }
  end

  context 'with invalid values' do
    let(:table) do
      described_class.new id: 'user',
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

    it { expect { table }.to raise_error ArgumentError, /contains undefined columns/ }
  end

  context 'with partial values' do
    let(:table) do
      described_class.new id: 'user',
        columns: [
          Masamune::Schema::Column.new(id: 'name', type: :string),
          Masamune::Schema::Column.new(id: 'description', type: :string)
        ],
        rows: [
          Masamune::Schema::Row.new(values: {
            name: 'registered',
            description: 'Registered'
          }),
          Masamune::Schema::Row.new(values: {
            name: 'active'
          })
        ]
    end

    it { expect(table.name).to eq('user_table') }
  end

  context 'with shared unique index' do
    let(:table) do
      described_class.new id: 'user',
        columns: [
          Masamune::Schema::Column.new(id: 'tenant_id', type: :integer, unique: 'tenant_and_user', index: 'tenant_and_user'),
          Masamune::Schema::Column.new(id: 'user_id', type: :integer, unique: 'tenant_and_user', index: 'tenant_and_user')
        ]
    end

    it { expect(table.name).to eq('user_table') }
  end

  context 'with multiple default and named rows' do
    let(:table) do
      described_class.new id: 'user',
        columns: [
          Masamune::Schema::Column.new(id: 'uuid', type: :uuid, surrogate_key: true),
          Masamune::Schema::Column.new(id: 'tenant_id', type: :integer, natural_key: true),
          Masamune::Schema::Column.new(id: 'user_id', type: :integer, natural_key: true)
        ],
        rows: [
          Masamune::Schema::Row.new(values: {
            tenant_id: 'default_tenant_id()',
            user_id: -1,
          }, default: true),
          Masamune::Schema::Row.new(values: {
            tenant_id: 'default_tenant_id()',
            user_id: -2,
          }, id: 'unknown')
        ]
    end

    it { expect(table.name).to eq('user_table') }
  end

  context 'with referenced tables with default values' do
    let(:mini_table) do
      described_class.new id: 'user_account_state',
        columns: [
          Masamune::Schema::Column.new(id: 'name', type: :string, unique: true),
          Masamune::Schema::Column.new(id: 'description', type: :string)
        ],
        rows: [
          Masamune::Schema::Row.new(values: {
            name: 'registered',
            description: 'Registered'
          }),
          Masamune::Schema::Row.new(values: {
            name: 'active',
            description: 'Active',
          }, default: true),
          Masamune::Schema::Row.new(values: {
            name: 'inactive',
            description: 'Inactive'
          })
        ]
    end

    let(:table) do
      described_class.new id: 'user', references: [Masamune::Schema::TableReference.new(mini_table)],
        columns: [
          Masamune::Schema::Column.new(id: 'name', type: :string)
        ]
    end

    it { expect(table.name).to eq('user_table') }
    it { expect(table.columns[:user_account_state_table_id].required_value?).to eq(false) }
  end

  context 'with labeled referenced table' do
    let(:mini_table) do
      described_class.new id: 'user_account_state',
        columns: [
          Masamune::Schema::Column.new(id: 'name', type: :string, unique: true),
          Masamune::Schema::Column.new(id: 'description', type: :string)
        ],
        rows: [
          Masamune::Schema::Row.new(values: {name: 'active'}, default: true)
        ]
    end

    let(:table) do
      described_class.new id: 'user', references: [
          Masamune::Schema::TableReference.new(mini_table),
          Masamune::Schema::TableReference.new(mini_table, label: 'actor', null: true, default: :null)
        ],
        columns: [
          Masamune::Schema::Column.new(id: 'name', type: :string)
        ]
    end

    it { expect(table.name).to eq('user_table') }
    it { expect(table.columns[:user_account_state_table_id].required_value?).to eq(false) }
    it { expect(table.columns[:actor_user_account_state_table_id].required_value?).to eq(false) }
  end

  context 'with referenced tables without default values' do
    let(:mini_table) do
      described_class.new id: 'user_account_state',
        columns: [
          Masamune::Schema::Column.new(id: 'name', type: :string, unique: true),
          Masamune::Schema::Column.new(id: 'description', type: :string)
        ]
    end

    let(:table) do
      described_class.new id: 'user', references: [Masamune::Schema::TableReference.new(mini_table)],
        columns: [
          Masamune::Schema::Column.new(id: 'name', type: :string)
        ]
    end

    it { expect(table.name).to eq('user_table') }
    it { expect(table.columns[:user_account_state_table_id].required_value?).to eq(true) }
  end

  context '#stage_table' do
    let(:mini_table) do
      described_class.new id: 'user_account_state',
        columns: [
          Masamune::Schema::Column.new(id: 'name', type: :string, unique: true),
          Masamune::Schema::Column.new(id: 'description', type: :string)
        ]
    end

    let(:table) do
      described_class.new id: 'user', references: [
          Masamune::Schema::TableReference.new(mini_table),
          Masamune::Schema::TableReference.new(mini_table, label: 'hr')
        ],
        columns: [
          Masamune::Schema::Column.new(id: 'user_id', type: :integer),
          Masamune::Schema::Column.new(id: 'name', type: :string),
          Masamune::Schema::Column.new(id: 'last_modified_at', type: :timestamp)
        ]
    end

    context 'without suffix or selected columns' do
      let!(:stage_table) { table.stage_table }

      it 'should duplicate columns' do
        expect(table.parent).to be_nil
        expect(table.columns[:name].parent).to eq(table)
        expect(stage_table.parent).to eq(table)
        expect(stage_table.columns[:name].parent).to eq(stage_table)
      end
    end

    context 'with optional suffix' do
      let!(:stage_table) { table.stage_table(suffix: 'actor') }

      it 'should append suffix to id' do
        expect(stage_table.id).to eq(:user_actor)
        expect(stage_table.name).to eq('user_actor_table_stage')
      end

      it 'should duplicate columns' do
        expect(table.parent).to be_nil
        expect(table.columns[:name].parent).to eq(table)
        expect(stage_table.parent).to eq(table)
        expect(stage_table.columns[:name].parent).to eq(stage_table)
      end
    end

    context 'with specified columns' do
      subject(:stage_table) { table.stage_table(columns: %w(id name user_account_state.id hr_user_account_state.id)) }

      it 'should stage table' do
        expect(stage_table.name).to eq('user_table_stage')
        expect(stage_table.columns.keys).to eq([:name, :user_account_state_table_id, :hr_user_account_state_table_id])
        expect(stage_table.references.keys).to eq([:user_account_state, :hr_user_account_state])
      end
    end

    context 'with specified columns (denormalized)' do
      subject(:stage_table) { table.stage_table(columns: %w(id name user_account_state.name hr_user_account_state.name)) }

      it 'should stage table' do
        expect(stage_table.name).to eq('user_table_stage')
        expect(stage_table.columns.keys).to eq([:name, :user_account_state_table_name, :hr_user_account_state_table_name])
        expect(stage_table.references.keys).to eq([:user_account_state, :hr_user_account_state])
      end
    end

    context 'with specified target table' do
      let(:target) do
        described_class.new id: 'user',
          columns: [
            Masamune::Schema::Column.new(id: 'name', type: :string),
            Masamune::Schema::Column.new(id: 'user_account_state.name', type: :string),
            Masamune::Schema::Column.new(id: 'hr_user_account_state.name', type: :string)
          ]
      end

      subject(:stage_table) { table.stage_table(target: target) }

      it 'should stage table' do
        expect(stage_table.name).to eq('user_table_stage')
        expect(stage_table.columns.keys).to eq([:name, :user_account_state_table_name, :hr_user_account_state_table_name])
        expect(stage_table.references.keys).to eq([:user_account_state, :hr_user_account_state])
      end
    end

    context 'with specified target table (referenced columns)' do
      let(:target) do
        described_class.new id: 'user', type: :stage,
          columns: [
            Masamune::Schema::Column.new(id: 'user_id', type: :integer),
            Masamune::Schema::Column.new(id: 'name', type: :string),
            Masamune::Schema::Column.new(id: 'name', type: :string, reference: Masamune::Schema::TableReference.new(mini_table, label: 'hr'))
          ]
      end

      subject(:stage_table) { table.stage_table(target: target) }

      it 'should stage table' do
        expect(stage_table.name).to eq('user_table_stage')
        expect(stage_table.columns.keys).to eq([:user_id, :name, :hr_user_account_state_table_name])
        expect(stage_table.references.keys).to eq([:hr_user_account_state])
      end
    end

    context 'with specified target table (referenced tables)' do
      let(:target) do
        described_class.new id: 'user_data', references: [
            Masamune::Schema::TableReference.new(mini_table, label: 'hr')
          ],
          columns: [
            Masamune::Schema::Column.new(id: 'user_id', type: :integer),
            Masamune::Schema::Column.new(id: 'name', type: :string)
          ]
      end

      subject(:stage_table) { table.stage_table(target: target) }

      it 'should stage table' do
        expect(stage_table.name).to eq('user_table_stage')
        expect(stage_table.columns.keys).to eq([:hr_user_account_state_table_id, :user_id, :name])
        expect(stage_table.references.keys).to eq([:hr_user_account_state])
      end
    end
  end
end
