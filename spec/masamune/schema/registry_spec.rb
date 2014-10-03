require 'spec_helper'
require 'active_support/core_ext/string/strip'

describe Masamune::Schema::Registry do
  let(:environment) { double }
  let(:instance) { described_class.new(environment) }

  describe '#schema' do
    context 'when schema contains dimensions' do
      before do
        instance.schema do
          dimension 'foo', type: :two
          dimension 'bar', type: :two
        end
      end

      subject { instance.dimensions }

      it { is_expected.to include :foo }
      it { is_expected.to include :bar }
    end

    context 'when schema contains columns' do
      before do
        instance.schema do
          dimension 'table_one', type: :two do
            column 'column_one'
            column 'column_two'
          end

          dimension 'table_two', type: :two do
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
          dimension 'table_one', type: :two do
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
          dimension 'foo', type: :one
          dimension 'bar', type: :one
          dimension 'baz', type: :two do
            references :foo
            references :bar, label: :quux
          end
        end
      end

      subject(:references) { instance.dimensions[:baz].references }

      it { is_expected.to include :foo }
      it { is_expected.to include :bar }
      it { expect(references[:foo].label).to be_nil }
      it { expect(references[:bar].label).to eq(:quux) }
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

    context 'when schema contains facts' do
      before do
        instance.schema do
          dimension 'dimension_one', type: :two do
            column 'column_one'
            column 'column_two'
          end

          fact 'fact_one' do
            references :dimension_one
            measure 'measure_one'
          end

          fact 'fact_two' do
            references :dimension_one
            measure 'measure_two'
          end
        end
      end

      let(:fact_one) { instance.facts[:fact_one] }
      let(:fact_two) { instance.facts[:fact_two] }

      it { expect(fact_one.references).to include :dimension_one}
      it { expect(fact_one.measures).to include :measure_one }
    end

    context 'when schema contains events' do
      before do
        instance.schema do
          event 'event_one' do
            attribute 'attribute_one'
            attribute 'attribute_two'
          end

          event 'event_two' do
            attribute 'attribute_three'
            attribute 'attribute_four'
          end
        end
      end

      let(:event_one) { instance.events[:event_one] }
      let(:event_two) { instance.events[:event_two] }

      it { expect(event_one.attributes).to include :attribute_one }
      it { expect(event_one.attributes).to include :attribute_two }
      it { expect(event_two.attributes).to include :attribute_three }
      it { expect(event_two.attributes).to include :attribute_four }
    end

    context 'when schema contains file' do
      before do
        instance.schema do
          dimension 'user_account', type: :mini do
            column 'name', type: :string
          end

          file 'users' do
            column 'user_account.name', type: :string
            column 'admin', type: :boolean
          end
        end
      end

      subject(:file) { instance.files[:users] }

      it 'should expect dot notation column names to reference dimensions' do
        expect(file.columns).to include :user_account_type_name
        expect(file.columns).to include :admin
        expect(file.columns[:user_account_type_name].reference).to eq(instance.dimensions[:user_account])
        expect(file.columns[:admin].reference).to be_nil
      end
    end

    context 'when schema contains file with invalid reference' do
      subject(:schema) do
        instance.schema do
          file 'users' do
            column 'user_account.name', type: :string
            column 'admin', type: :boolean
          end
        end
      end

      it 'should raise an exception' do
        expect { schema }.to raise_error /dimension user_account not defined/
      end
    end

    context 'when schema contains map' do
      before do
        instance.schema do
          dimension 'user_account_state', type: :mini do
            column 'name', type: :string
          end

          file 'users' do; end

          map from: files[:users], to: dimensions[:user_account_state] do
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

      subject(:map) { instance.files[:users].map(to: instance.dimensions[:user_account_state]) }

      it 'constructs map' do
        expect(map.fields[:tenant_id]).to eq('tenant_id')
        expect(map.fields[:user_id]).to eq('id')
        expect(map.fields[:'user_account_state.name']).to be_a(Proc)
        expect(map.fields[:start_at]).to eq('updated_at')
        expect(map.fields[:delta]).to eq(0)
      end
    end

    context 'when schema contains map missing the from: field' do
      subject(:schema) do
        instance.schema do
          map do
            field 'tenant_id'
          end
        end
      end

      it 'should raise an exception' do
        expect { schema }.to raise_error /invalid map, from: is missing/
      end
    end

    context 'when schema contains map missing the to: field' do
      subject(:schema) do
        instance.schema do
          file 'users' do; end

          map from: files[:users] do
            field 'tenant_id'
          end
        end
      end

      it 'should raise an exception' do
        expect { schema }.to raise_error /invalid map from: 'users', to: is missing/
      end
    end
  end
end
