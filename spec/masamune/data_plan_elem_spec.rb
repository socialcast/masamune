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
    it { should == 'report/2013-07-19/11' }
  end

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
