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

describe Masamune::Schema::Fact do
  let(:store) { double(id: 'store', type: :postgres) }

  let(:date_dimension) do
    Masamune::Schema::Dimension.new id: 'date', type: :date,
      columns: [
        Masamune::Schema::Column.new(id: 'date_id')
      ]
  end

  let(:user_dimension) do
    Masamune::Schema::Dimension.new id: 'user', type: :two,
      columns: [
        Masamune::Schema::Column.new(id: 'tenant_id', index: true),
        Masamune::Schema::Column.new(id: 'user_id', index: true)
      ]
  end

  let(:fact_without_partition) do
    described_class.new id: 'visits', store: store,
      references: [
        Masamune::Schema::TableReference.new(date_dimension),
        Masamune::Schema::TableReference.new(user_dimension)
      ],
      columns: [
        Masamune::Schema::Column.new(id: 'total', type: :integer)
      ]
  end

  let(:fact_with_partition) do
    described_class.new id: 'visits', store: store, partition: 'y%Ym%m',
      references: [
        Masamune::Schema::TableReference.new(date_dimension),
        Masamune::Schema::TableReference.new(user_dimension)
      ],
      columns: [
        Masamune::Schema::Column.new(id: 'total', type: :integer),
        Masamune::Schema::Column.new(id: 'y', type: :integer, partition: true),
        Masamune::Schema::Column.new(id: 'm', type: :integer, partition: true)
      ]
  end

  let(:fact_with_partition_and_hourly_grain) do
    described_class.new id: 'visits', store: store, grain: :hourly, partition: 'y%Ym%m',
      references: [
        Masamune::Schema::TableReference.new(date_dimension),
        Masamune::Schema::TableReference.new(user_dimension)
      ],
      columns: [
        Masamune::Schema::Column.new(id: 'total', type: :integer)
      ]
  end

  it { expect(fact_without_partition.name).to eq('visits_fact') }
  it { expect(fact_with_partition.name).to eq('visits_fact') }
  it { expect(fact_with_partition_and_hourly_grain.id).to eq(:visits_hourly) }
  it { expect(fact_with_partition_and_hourly_grain.name).to eq('visits_hourly_fact') }

  context 'fact with unknown grain' do
    subject(:fact) do
      described_class.new id: 'visits', grain: :quarterly
    end

    it { expect { fact }.to raise_error ArgumentError, "unknown grain 'quarterly'" }
  end

  describe '#partition_table' do
    let(:date) { Chronic.parse('2015-01-01') }
    subject(:partition_table) { fact.partition_table(date) }

    context 'fact without partition' do
      let(:fact) { fact_without_partition }

      it { expect(partition_table).to be_nil }
    end

    context 'fact with partition' do
      let(:fact) { fact_with_partition }

      it { expect(partition_table.store.id).to eq(store.id) }
      it { expect(partition_table.name).to eq('visits_fact_y2015m01') }
      it { expect(partition_table.range.start_date).to eq(date.utc.to_date) }

      describe '#stage_table' do
        subject(:stage_table) { partition_table.stage_table }

        it { expect(stage_table.store.id).to eq(store.id) }
        it { expect(stage_table.name).to eq('visits_fact_y2015m01_stage') }
        it { expect(stage_table.range.start_date).to eq(date.utc.to_date) }

        context 'with optional suffix' do
          subject(:stage_table) { partition_table.stage_table(suffix: 'tmp') }

          it 'should append suffix to id' do
            expect(stage_table.store.id).to eq(store.id)
            expect(stage_table.name).to eq('visits_fact_y2015m01_stage_tmp')
            expect(stage_table.range.start_date).to eq(date.utc.to_date)
          end
        end
      end
    end

    context 'fact with partition and hourly grain' do
      let(:fact) { fact_with_partition_and_hourly_grain }

      it { expect(partition_table.store.id).to eq(store.id) }
      it { expect(partition_table.name).to eq('visits_hourly_fact_y2015m01') }
      it { expect(partition_table.grain).to eq(fact.grain) }
      it { expect(partition_table.range.start_date).to eq(date.utc.to_date) }

      describe '#stage_table' do
        subject(:stage_table) { partition_table.stage_table }

        it { expect(stage_table.store.id).to eq(store.id) }
        it { expect(stage_table.name).to eq('visits_hourly_fact_y2015m01_stage') }
        it { expect(stage_table.grain).to eq(fact.grain) }
        it { expect(stage_table.range.start_date).to eq(date.utc.to_date) }
      end
    end
  end

  describe '#partition_tables' do
    let(:start_date) { Date.civil(2015, 01, 01) }
    let(:stop_date) { Date.civil(2015, 03, 15) }

    subject(:partition_tables) { fact.partition_tables(start_date, stop_date) }

    context 'fact without partition' do
      let(:fact) { fact_without_partition }

      it { expect(partition_tables).to be_nil }
    end

    context 'fact with partition' do
      let(:fact) { fact_with_partition }

      it 'yields partition tables' do
        expect { |b| fact.partition_tables(start_date, stop_date, &b) }.to yield_successive_args \
           fact.partition_table(Date.civil(2015, 01, 01)),
           fact.partition_table(Date.civil(2015, 02, 01)),
           fact.partition_table(Date.civil(2015, 03, 01))
      end
    end
  end
end
