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

describe Masamune::Schema::Catalog do
  let(:environment) { double }
  let(:instance) { described_class.new(environment) }
  let(:postgres) { instance.postgres }
  let(:hive) { instance.hive }
  let(:files) { instance.files }

  describe '#method_missing' do
    before do
      instance.schema :postgres do
        dimension 'foo', type: :two
      end
    end

    it { expect(postgres.foo_dimension.id).to eq(:foo) }
    it { expect(postgres.bar_dimension).to be_nil }
    it { expect { postgres.foo_baz }.to raise_error NoMethodError }
  end

  describe '#[]' do
    context 'with :postgres' do
      subject { instance[:postgres] }
      it { is_expected.to eq(postgres) }
    end
    context 'with :hive' do
      subject { instance[:hive] }
      it { is_expected.to eq(hive) }
    end
    context 'with :mysql' do
      subject { instance[:mysql] }
      it { expect { subject }.to raise_error ArgumentError, "unknown type: 'mysql'" }
    end
  end

  context '#load' do
    let(:postgres_extra) { %w(/tmp/schema.psql /tmp/00_schema.psql /tmp/20_schema.psql /tmp/40_schema.psql.erb) }
    let(:hive_extra) { %w(/tmp/schema.hql /tmp/00_schema.hql /tmp/20_schema.hql /tmp/40_schema.hql.erb) }
    let(:extra) { postgres_extra + hive_extra }

    before do
      extra.each do |e|
        instance.load(e)
      end
    end

    it 'should load postgres extra in order' do
      expect(instance.postgres.extra).to eq(postgres_extra)
      expect(instance.postgres.extra(:pre).size).to eq(2)
      expect(instance.postgres.extra(:post).size).to eq(2)
    end

    it 'should load hive extra in order' do
      expect(instance.hive.extra).to eq(hive_extra)
      expect(instance.hive.extra(:pre).size).to eq(2)
      expect(instance.hive.extra(:post).size).to eq(2)
    end
  end

  describe '#schema' do
    context 'when schema does not define store' do
      subject(:schema) do
        instance.schema do
          dimension 'foo', type: :two
          dimension 'bar', type: :two
        end
      end

      it { expect { schema }.to raise_error ArgumentError, 'schema store arguments required' }
    end

    context 'when schema defines unknown store' do
      subject(:schema) do
        instance.schema :mysql do
          dimension 'foo', type: :two
          dimension 'bar', type: :two
        end
      end

      it { expect { schema }.to raise_error ArgumentError, "unknown type: 'mysql'" }
    end

    context 'when schema contains dimensions' do
      before do
        instance.schema :postgres do
          dimension 'foo', type: :two
          dimension 'bar', type: :two
        end
      end

      it { expect(postgres.dimensions).to include :foo }
      it { expect(postgres.dimensions).to include :bar }
      it { expect(postgres.foo_dimension.id).to eq(:foo) }
      it { expect(postgres.bar_dimension.id).to eq(:bar) }
    end

    context 'when schema contains columns' do
      before do
        instance.schema :postgres do
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

      let(:table_one_columns) { postgres.table_one_dimension.columns }
      let(:table_two_columns) { postgres.table_two_dimension.columns }

      it { expect(table_one_columns).to include :column_one }
      it { expect(table_one_columns).to include :column_two }
      it { expect(table_one_columns).to_not include :column_three }
      it { expect(table_one_columns).to_not include :column_four }
      it { expect(table_two_columns).to_not include :column_one }
      it { expect(table_two_columns).to_not include :column_two }
      it { expect(table_two_columns).to include :column_three }
      it { expect(table_two_columns).to include :column_four }
    end

    context 'when schema contains columns with symbol names' do
      before do
        instance.schema :postgres do
          dimension :table_one, type: :two do
            column :column_one
          end
        end
      end

      let(:table_one_columns) { postgres.table_one_dimension.columns }

      it { expect(table_one_columns).to include :column_one }
    end

    context 'when schema contains columns and rows' do
      before do
        instance.schema :postgres do
          dimension 'table_one', type: :two do
            column 'column_one', type: :integer
            column 'column_two', type: :string
            row column_one: 1, column_two: 'a'
            row column_one: 2, column_two: 'b'
          end
        end
      end

      let(:table_one_rows) { postgres.table_one_dimension.rows }

      it { expect(table_one_rows[0].values).to include(column_one: 1, column_two: 'a') }
      it { expect(table_one_rows[1].values).to include(column_one: 2, column_two: 'b') }
    end

    context 'when schema contains references' do
      before do
        instance.schema :postgres do
          dimension 'foo', type: :one
          dimension 'bar', type: :one
          dimension 'baz', type: :two do
            references :foo
            references :bar, label: :quux
          end
        end
      end

      subject(:references) { postgres.baz_dimension.references }

      it { is_expected.to include :foo }
      it { is_expected.to include :quux_bar }
      it { expect(references[:foo].label).to be_nil }
      it { expect(references[:quux_bar].label).to eq(:quux) }
    end

    context 'when schema contains overrides' do
      before do
        instance.schema :postgres do
          dimension 'cluster', type: :mini do
            column 'uuid', type: :uuid, surrogate_key: true
            column 'name', type: :string, unique: true
            column 'description', type: :string

            row name: 'current_database()', attributes: { default: true }
          end
        end
      end

      subject { postgres.cluster_dimension.columns }

      it { is_expected.to include :uuid }
      it { is_expected.to_not include :id }
    end

    context 'when schema contains facts' do
      before do
        instance.schema :postgres do
          dimension 'dimension_one', type: :two do
            column 'column_one'
            column 'column_two'
          end

          fact 'fact_one' do
            references :dimension_one
            measure 'measure_one', aggregate: :sum
          end

          fact 'fact_two' do
            references :dimension_one
            measure 'measure_two', aggregate: :average
          end
        end
      end

      let(:fact_one) { postgres.fact_one_fact }
      let(:fact_two) { postgres.fact_two_fact }

      it { expect(fact_one.references).to include :dimension_one }
      it { expect(fact_one.measures).to include :measure_one }
      it { expect(fact_one.measures[:measure_one].aggregate).to eq(:sum) }
      it { expect(fact_two.references).to include :dimension_one }
      it { expect(fact_two.measures).to include :measure_two }
      it { expect(fact_two.measures[:measure_two].aggregate).to eq(:average) }
    end

    context 'when schema contains fact with partition table' do
      before do
        instance.schema :hive do
          fact 'visits', partition: 'y%Ym%m' do
            measure 'count', aggregate: :sum
          end
        end
      end

      it { expect(hive.visits_fact.partition).to eq('y%Ym%m') }
      it { expect(hive.visits_fact.measures).to include :count }
      it { expect(hive.visits_fact.measures[:count].aggregate).to eq(:sum) }
    end

    context 'when schema contains fact with partition columns' do
      before do
        instance.schema :hive do
          fact 'visits' do
            partition 'y', type: :integer
            partition 'm', type: :integer
            partition 'd', type: :integer
            measure 'count', aggregate: :sum
          end
        end
      end

      it { expect(hive.visits_fact.partitions).to include :y }
      it { expect(hive.visits_fact.partitions).to include :m }
      it { expect(hive.visits_fact.partitions).to include :d }
      it { expect(hive.visits_fact.measures).to include :count }
      it { expect(hive.visits_fact.measures[:count].aggregate).to eq(:sum) }
    end

    context 'when schema contains fact with degenerate dimension references' do
      before do
        instance.schema :hive do
          fact 'visits' do
            references :message_kind, degenerate: true
            measure 'count', aggregate: :sum
          end
        end
      end

      it { expect(hive.visits_fact.references).to include :message_kind }
      it { expect(hive.visits_fact.columns).to include :message_kind_type_id }
      it { expect(hive.visits_fact.measures).to include :count }
      it { expect(hive.visits_fact.measures[:count].aggregate).to eq(:sum) }
    end

    context 'when schema contains fact with a single grain' do
      before do
        instance.schema :postgres do
          dimension 'user', type: :two do
            column 'user_id'
          end

          fact 'visits', grain: 'hourly' do
            references :user
            measure 'count'
          end
        end
      end

      let(:visits_hourly) { postgres.visits_hourly_fact }

      it { expect(visits_hourly.name).to eq('visits_hourly_fact') }
      it { expect(visits_hourly.references).to include :user }
      it { expect(visits_hourly.measures).to include :count }
    end

    context 'when schema contains fact with multiple grain' do
      before do
        instance.schema :postgres do
          dimension 'user', type: :two do
            column 'user_id'
          end

          fact 'visits', grain: %w(hourly daily monthly) do
            references :user
            measure 'count'
          end
        end
      end

      let(:visits_hourly) { postgres.visits_hourly_fact }
      let(:visits_daily) { postgres.visits_daily_fact }
      let(:visits_monthly) { postgres.visits_monthly_fact }

      it { expect(visits_hourly.name).to eq('visits_hourly_fact') }
      it { expect(visits_hourly.references).to include :user }
      it { expect(visits_hourly.measures).to include :count }
      it { expect(visits_daily.name).to eq('visits_daily_fact') }
      it { expect(visits_daily.references).to include :user }
      it { expect(visits_daily.measures).to include :count }
      it { expect(visits_monthly.name).to eq('visits_monthly_fact') }
      it { expect(visits_monthly.references).to include :user }
      it { expect(visits_monthly.measures).to include :count }
    end

    context 'when schema contains file' do
      before do
        instance.schema :postgres do
          dimension 'user_account', type: :mini do
            column 'name', type: :string
          end
        end

        instance.schema :files do
          file 'users' do
            column 'postgres.user_account.name', type: :string
            column 'admin', type: :boolean
          end
        end
      end

      subject(:file) { files.users }

      it 'should expect dot notation column names to reference dimensions' do
        expect(file.columns).to include :user_account_type_name
        expect(file.columns).to include :admin
        expect(file.columns[:user_account_type_name].reference).to eq(postgres.dimensions[:user_account])
        expect(file.columns[:admin].reference).to be_nil
      end
    end

    context 'when schema contains file with headers & format override' do
      before do
        instance.schema :postgres do
          file 'override', headers: false, format: :tsv
          file 'default'
        end
      end

      it 'should override store format' do
        expect(postgres.headers).to eq(true)
        expect(postgres.format).to eq(:csv)
        expect(postgres.override_file.store.headers).to eq(false)
        expect(postgres.override_file.store.format).to eq(:tsv)
        expect(postgres.default_file.store.headers).to eq(true)
        expect(postgres.default_file.store.format).to eq(:csv)
      end
    end

    context 'when schema contains file with invalid reference' do
      subject(:schema) do
        instance.schema :postgres do
          file 'users' do
            column 'user_account.name', type: :string
            column 'admin', type: :boolean
          end
        end
      end

      it 'should raise an exception' do
        expect { schema }.to raise_error(/dimension user_account not defined/)
      end
    end

    context 'when schema contains map from: file' do
      before do
        instance.schema :postgres do
          dimension 'user_account_state', type: :mini do
            column 'name', type: :string
          end

          dimension 'user', type: :two do
            references :user_account_state
            column 'tenant_id', type: :integer, natural_key: true
            column 'user_id', type: :integer, natural_key: true
          end
        end

        instance.schema :files do
          file 'users' do
            column 'id', type: :integer
            column 'tenant_id', type: :integer
            column 'updated_at', type: :timestamp
            column 'deleted_at', type: :timestamp
          end

          map from: files.users, to: postgres.user_dimension do |row|
            {
              'tenant_id'               => row[:tenant_id],
              'user_id'                 => row[:id],
              'user_account_state.name' => row[:deleted_at] ? 'deleted' : 'active',
              'start_at'                => row[:updated_at],
              'delta'                   => 0
            }
          end
        end
      end

      subject(:map) { files.users.map(to: postgres.user_dimension) }

      it 'constructs map' do
        expect(map.function).to_not be_nil
      end
    end

    context 'when schema contains map missing the from: field' do
      subject(:schema) do
        instance.schema :postgres do
          map do |row|
            {
              id: row[:id]
            }
          end
        end
      end

      it 'should raise an exception' do
        expect { schema }.to raise_error(/invalid map, from: is missing/)
      end
    end

    context 'when schema contains map with invalid options' do
      subject(:schema) do
        instance.schema :postgres do
          map :x do |row|
            {
              id: row[:id]
            }
          end
        end
      end

      it 'should raise an exception' do
        expect { schema }.to raise_error(/invalid map, from: is missing/)
      end
    end

    context 'when schema contains map missing the to: field' do
      subject(:schema) do
        instance.schema :postgres do
          file 'users'

          map from: postgres.users_file do
            field 'tenant_id'
          end
        end
      end

      it 'should raise an exception' do
        expect { schema }.to raise_error(/invalid map from: 'users', to: is missing/)
      end
    end

    context 'when schema addressed with symbols' do
      before do
        instance.schema :postgres do
          dimension 'user', type: :one
          file 'users'

          map from: postgres.files[:users], to: postgres.dimensions[:user] do
            field 'tenant_id'
          end
        end
      end

      subject(:map) { postgres.files[:users].map(to: postgres.dimensions[:user]) }

      it 'should construct map' do
        is_expected.to_not be_nil
      end
    end

    context 'when schema addressed with strings' do
      before do
        instance.schema :postgres do
          dimension 'user', type: :one
          file 'users'

          map from: postgres.files['users'], to: postgres.dimensions['user'] do
            field 'tenant_id'
          end
        end
      end

      subject(:map) { postgres.files['users'].map(to: postgres.dimensions['user']) }

      it 'should construct map' do
        is_expected.to_not be_nil
      end
    end
  end

  describe '.dereference_column' do
    before do
      instance.schema :postgres do
        dimension 'table_one', type: :two do
          column 'column_one'
        end

        dimension 'table_two', type: :two do
          references :table_one
          references :table_one, label: :label_one

          column 'column_two'
        end
      end
    end

    subject(:result) { postgres.dereference_column(input) }

    context 'with a column name' do
      let(:input) { 'column_two' }
      it { expect(result.name).to eq(:column_two) }
    end

    context 'with a table.column name' do
      let(:input) { 'table_one.column_one' }
      it { expect(result.name).to eq(:table_one_dimension_column_one) }
    end

    context 'with a labeled table.column name' do
      let(:input) { 'label_one_table_one.column_one' }
      it { expect(result.name).to eq(:label_one_table_one_dimension_column_one) }
    end

    context 'with a undefined table.column name' do
      let(:input) { 'undef.column_one' }
      it { expect { result }.to raise_error ArgumentError, /dimension undef not defined/ }
    end
  end
end
