require 'spec_helper'
require 'active_support/core_ext/string/strip'

describe Masamune::Schema::Registry do
  let(:environment) { double }
  let(:instance) { described_class.new(environment) }

  describe '#schema' do
    context 'when schema contains dimensions' do
      before do
        instance.schema do
          dimension name: 'foo'
          dimension name: 'bar'
        end
      end

      subject { instance.dimensions }

      it { is_expected.to include :foo }
      it { is_expected.to include :bar }
    end

    context 'when schema contains columns' do
      before do
        instance.schema do
          dimension name: 'table_one' do
            column name: 'column_one'
            column name: 'column_two'
          end

          dimension name: 'table_two' do
            column name: 'column_three'
            column name: 'column_four'
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
          dimension name: 'table_one' do
            column name: 'column_one', type: :integer
            column name: 'column_two', type: :string
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
          dimension name: 'foo'
          dimension name: 'bar'
          dimension name: 'baz' do
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
          dimension name: 'cluster', type: :mini do
            column name: 'uuid', type: :uuid, primary_key: true
            column name: 'name', type: :string, unique: true
            column name: 'description', type: :string

            row name: 'current_database()', attributes: {default: true}
          end
        end
      end

      subject { instance.dimensions[:cluster].columns }

      it { is_expected.to include :uuid }
      it { is_expected.to_not include :id }
    end
  end
end
