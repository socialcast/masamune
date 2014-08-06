require 'spec_helper'
require 'active_support/core_ext/string/strip'

describe Masamune::Schema::Registry do
  let(:environment) { double }
  let(:instance) { described_class.new(environment) }

  describe '#schema' do
    context 'when schema contains dimensions' do
      before do
        instance.schema do
          dimension 'foo'
          dimension 'bar'
        end
      end

      subject { instance.dimensions }

      it { is_expected.to include :foo }
      it { is_expected.to include :bar }
    end

    context 'when schema contains columns' do
      before do
        instance.schema do
          dimension 'table_one' do
            column 'column_one'
            column 'column_two'
          end

          dimension 'table_two' do
            column 'column_three'
            column 'column_four'
          end
        end
      end

      let(:table_one_columns) { instance.dimensions[:table_one].columns }
      let(:table_two_columns) { instance.dimensions[:table_two].columns }

      it { expect(table_one_columns).to include :column_one }
      it { expect(table_one_columns).to include :column_two }
      it { expect(table_one_columns).to_not include :column_three }
      it { expect(table_one_columns).to_not include :column_four }
      it { expect(table_two_columns).to_not include :column_one }
      it { expect(table_two_columns).to_not include :column_two }
      it { expect(table_two_columns).to include :column_three }
      it { expect(table_two_columns).to include :column_four }
    end

    context 'when schema contains columns and rows' do
      before do
        instance.schema do
          dimension 'table_one' do
            column 'column_one', type: :integer
            column 'column_two', type: :string
            row column_one: 1, column_two: 'a'
            row column_one: 2, column_two: 'b'
          end
        end
      end

      let(:table_one_rows) { instance.dimensions[:table_one].rows }

      it { expect(table_one_rows[0].values).to include(column_one: 1, column_two: 'a') }
      it { expect(table_one_rows[1].values).to include(column_one: 2, column_two: 'b') }
    end

    context 'when schema contains references' do
      before do
        instance.schema do
          dimension 'foo'
          dimension 'bar'
          dimension 'baz' do
            references :foo
            references :bar
          end
        end
      end

      subject { instance.dimensions[:baz].references }

      it { is_expected.to include :foo }
      it { is_expected.to include :bar }
    end

    context 'when schema contains overrides' do
      before do
        instance.schema do
          dimension 'cluster', type: :mini do
            column 'uuid', type: :uuid, primary_key: true
            column 'name', type: :string, unique: true
            column 'description', type: :string

            row name: 'current_database()', attributes: {default: true}
          end
        end
      end

      subject { instance.dimensions[:cluster].columns }

      it { is_expected.to include :uuid }
      it { is_expected.to_not include :id }
    end

    context 'when schema contains csv files' do
      before do
        instance.schema do
          file 'users' do
            column 'user_account_type.name', type: :string
          end
        end
      end

      subject(:file) { instance.files[:users] }

      it 'should expect dot notation column names to references' do
        expect(file.columns).to include :'user_account_type.name'
      end
    end

    context 'when schema contains map' do
      before do
        instance.schema do
          map :user_csv_to_dimension do
            field 'tenant_id'
            field 'user_id', 'id'
            field 'user_account_state.name' do |row|
              row[:deleted_at] ? 'deleted' : 'active'
            end
            field 'start_at', 'updated_at'
            field 'delta', 0
          end
        end
      end

      subject(:map) { instance.maps[:user_csv_to_dimension] }

      it 'constructs map' do
        expect(map.name).to eq(:user_csv_to_dimension)
        expect(map.fields[:tenant_id]).to eq('tenant_id')
        expect(map.fields[:user_id]).to eq('id')
        expect(map.fields[:'user_account_state.name']).to be_a(Proc)
        expect(map.fields[:start_at]).to eq('updated_at')
        expect(map.fields[:delta]).to eq(0)
      end
    end
  end
end
