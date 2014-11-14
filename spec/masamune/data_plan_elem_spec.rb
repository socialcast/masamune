require 'spec_helper'

describe Masamune::DataPlanElem do
  let(:plan) { Masamune::DataPlan.new }
  let(:name) { 'primary' }
  let(:type) { :target }
  let(:rule) { Masamune::DataPlanRule.new(plan, name, type, {path: 'report/%Y-%m-%d/%H'}) }
  let(:other_rule) { Masamune::DataPlanRule.new(plan, name, type, {path: 'log/%Y%m%d.*.log'}) }

  let(:start_time) { DateTime.civil(2013,07,19,11,07) }
  let(:other_start_time) { DateTime.civil(2013,07,20,0,0) }

  let(:options) { {tz: 'EST'} }
  let(:other_options) { {tz: 'PST'} }

  let(:instance) { described_class.new(rule, start_time, options) }

  describe '#path' do
    subject do
      instance.path
    end
    it { is_expected.to eq('report/2013-07-19/11') }
  end

  describe '#==' do
    subject do
      instance == other
    end

    context 'when rule, options, and start_time match' do
      let(:other) { described_class.new(rule, start_time, options) }
      it { is_expected.to eq(true) }
      it 'should have same hash' do
        expect(instance.hash).to eq(other.hash)
      end
    end

    context 'when rules differ' do
      let(:other) { described_class.new(other_rule, start_time) }
      it { is_expected.to eq(false) }
    end

    context 'when options differ' do
      let(:other) { described_class.new(rule, start_time, other_options) }
      it { is_expected.to eq(false) }
    end

    context 'when start_times differ' do
      let(:other) { described_class.new(rule, other_start_time) }
      it { is_expected.to eq(false) }
    end
  end

  describe '#last_modified_at' do
    let(:early) { Time.parse("2014-05-01 00:00:00 +0000") }
    let(:later) { Time.parse("2014-06-01 00:00:00 +0000") }

    subject do
      instance.last_modified_at.utc
    end

    context 'with missing mtime' do
      before do
        expect(rule.plan.filesystem).to receive(:stat).with(instance.path).
          and_return(nil)
      end

      it { is_expected.to eq(Masamune::DataPlanElem::MISSING_MODIFIED_AT) }
    end

    context 'with single mtime' do
      before do
        expect(rule.plan.filesystem).to receive(:stat).with(instance.path).
          and_return(OpenStruct.new(mtime: early))
      end

      it { is_expected.to eq(early) }
    end
  end
end
