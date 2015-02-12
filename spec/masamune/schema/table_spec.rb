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

  context 'with referenced tables' do
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
  end

  context 'stage table' do
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

    let!(:stage_table) { table.stage_table }

    it 'should duplicate columns' do
      expect(table.parent).to be_nil
      expect(table.columns[:name].parent).to eq(table)
      expect(stage_table.parent).to eq(table)
      expect(stage_table.columns[:name].parent).to eq(stage_table)
    end

    context 'stage_table with optional suffix' do
      let!(:stage_table) { table.stage_table('actor') }

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
  end

  context '#as_file' do
    let(:store) { double(type: :postgres, headers: true) }

    let(:mini_table) do
      described_class.new id: 'user_account_state',
        columns: [
          Masamune::Schema::Column.new(id: 'name', type: :string, unique: true),
          Masamune::Schema::Column.new(id: 'description', type: :string)
        ]
    end

    let(:table) do
      described_class.new id: 'user', store: store, references: [
          Masamune::Schema::TableReference.new(mini_table),
          Masamune::Schema::TableReference.new(mini_table, label: :hr)
        ],
        columns: [
          Masamune::Schema::Column.new(id: 'name', type: :string)
        ]
    end

    context 'without specified columns' do
      let(:file) { table.as_file }

      it { expect(file.as_table(table).name).to eq('user_table_file') }
    end

    context 'with all specified columns' do
      let(:file) { table.as_file(%w(uuid hr_user_account_state_table_uuid user_account_state_table_uuid name)) }

      it { expect(file.as_table(table).name).to eq('user_table_file') }
    end

    context 'with all specified columns in denormalized form' do
      let(:file) { table.as_file(%w(uuid hr_user_account_state.name user_account_state.name name)) }

      it { expect(file.as_table(table).name).to eq('user_table_file') }
    end
  end
end
