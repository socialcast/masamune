require 'spec_helper'

describe Masamune::DataPlanElem do

  let(:rule) { Masamune::DataPlanRule.new('report/%Y-%m-%d/%H') }
  let(:other_rule) { Masamune::DataPlanRule.new('table/%Y-%m-%d') }
  let(:wildcard_rule) { Masamune::DataPlanRule.new('log/%Y%m%d*.log', wildcard: true) }

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

    context 'with wildcard rule' do
      let(:input_path) { 'log/20120420.random.log' }
      let(:instance) { described_class.new(wildcard_rule, start_time, options.merge(wildcard: true, input_path: input_path)) }

      context 'when input_paths match' do
        let(:other) { described_class.new(wildcard_rule, other_start_time, options.merge(wildcard: true, input_path: input_path)) }
        it { should be_true }
        it 'should have same hash' do
          instance.hash.should == other.hash
        end
      end

      context 'when input_paths differ' do
        let(:other_input_path) { 'log/20120421.random.log' }
        let(:other) { described_class.new(wildcard_rule, other_start_time, options.merge(wildcard: true, input_path: other_input_path)) }
        it { should be_false }
      end
    end
  end
end
