require 'spec_helper'

describe Masamune::Schema::Fact do
  let(:store) { double(id: 'store') }

  let(:dimension) do
    Masamune::Schema::Dimension.new id: 'user', type: :two,
      columns: [
        Masamune::Schema::Column.new(id: 'tenant_id', index: true),
        Masamune::Schema::Column.new(id: 'user_id', index: true)
      ]
  end

  let(:fact) do
    described_class.new id: 'visits', store: store, partition: 'y%Ym%m',
      references: [Masamune::Schema::TableReference.new(dimension)],
      columns: [
        Masamune::Schema::Column.new(id: 'total', type: :integer)
      ]
  end

  it { expect(fact.name).to eq('visits_fact') }

  describe '#partition_table' do
    let(:date) { Chronic.parse('2015-01-01') }

    subject(:partition_table) { fact.partition_table(date) }

    it { expect(partition_table.store.id).to eq(store.id) }
    it { expect(partition_table.name).to eq('visits_fact_y2015m01') }
    it { expect(partition_table.range.start_date).to eq(date.utc.to_date) }

    describe '#stage_table' do
      subject(:stage_table) { partition_table.stage_table }

      it { expect(stage_table.store.id).to eq(store.id) }
      it { expect(stage_table.name).to eq('visits_fact_y2015m01_stage') }
      it { expect(stage_table.range.start_date).to eq(date.utc.to_date) }
    end
  end

  context 'fact with unknown grain' do
    subject(:fact) do
      described_class.new id: 'visits', grain: :quarterly
    end

    it { expect { fact }.to raise_error ArgumentError, "unknown grain 'quarterly'" }
  end

  context 'fact with :hourly grain' do
    let(:fact) do
      described_class.new id: 'visits', store: store, grain: :hourly, partition: 'y%Ym%m',
        references: [Masamune::Schema::TableReference.new(dimension)],
        columns: [
          Masamune::Schema::Column.new(id: 'total', type: :integer)
        ]
    end

    it { expect(fact.id).to eq(:visits_hourly) }
    it { expect(fact.name).to eq('visits_hourly_fact') }
  end
end
