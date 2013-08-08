require 'spec_helper'

describe Masamune::DataPlanElem do

  let(:plan) { Masamune::DataPlan.new }
  let(:type) { :target }
  let(:rule) { Masamune::DataPlanRule.new(plan, type, 'report/%Y-%m-%d/%H') }
  let(:other_rule) { Masamune::DataPlanRule.new(plan, type, 'table/%Y-%m-%d') }
  let(:wildcard_rule) { Masamune::DataPlanRule.new(plan, type, 'log/%Y%m%d*.log', wildcard: true) }

  let(:start_time) { Time.now }
  let(:other_start_time) { Time.now + 1.day }

  let(:options) { {tz: 'EST'} }
  let(:other_options) { {tz: 'PST'} }

  let(:instance) { described_class.new(rule, start_time, options) }

  describe '#==' do
    subject do
      instance == other
    end

    context 'when rule, options, and start_time match' do
      let(:other) { described_class.new(rule, start_time, options) }
      it { should be_true }
      it 'should have same hash' do
        instance.hash.should == other.hash
      end
    end

    context 'when rules differ' do
      let(:other) { described_class.new(other_rule, start_time) }
      it { should be_false }
    end

    context 'when options differ' do
      let(:other) { described_class.new(rule, start_time, other_options) }
      it { should be_false }
    end

    context 'when start_times differ' do
      let(:other) { described_class.new(rule, other_start_time) }
      it { should be_false }
    end
  end
end
