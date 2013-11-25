require 'spec_helper'

describe Masamune::DataPlanRule do
  let(:plan) { Masamune::DataPlan.new }
  let(:name) { 'primary' }
  let(:type) { :target }
  let(:pattern) { 'report/%Y-%m-%d/%H' }
  let(:options) { {} }

  let(:instance) { described_class.new(plan, name, type, pattern, options) }

  describe '#pattern' do
    subject do
      instance.pattern
    end

    context 'with string' do
      let(:pattern) { 'report/%Y-%m-%d/%H' }
      it { should == 'report/%Y-%m-%d/%H' }
    end

    context 'with lambda' do
      let(:pattern) { lambda { |_| 'report/%Y-%m-%d/%H' } }
      it { should == 'report/%Y-%m-%d/%H' }
    end
  end

  describe '#bind_date' do
    subject do
      instance.bind_date(input_date)
    end

    context 'with default' do
      let(:input_date) { DateTime.civil(2013,04,05,23,13) }

      its(:path) { should == 'report/2013-04-05/23' }
      let(:start_time) { DateTime.civil(2013,04,05,23) }
      let(:stop_time) { DateTime.civil(2013,04,05,0) }
    end

    context 'with unix timestamp pattern' do
      let(:pattern) { 'logs/%H-s.log' }
      let(:input_date) { DateTime.civil(2013,04,05,23,13) }

      its(:path) { should == 'logs/1365202800.log' }
      let(:start_time) { DateTime.civil(2013,04,05,23) }
      let(:stop_time) { DateTime.civil(2013,04,05,0) }
    end
  end

  describe '#bind_path' do
    subject do
      instance.bind_path(input_path)
    end

    context 'with default' do
      let(:input_path) { 'report/2013-04-05/23' }
      let(:output_date) { DateTime.civil(2013,04,05,23) }

      its(:path) { should == input_path }
      its(:start_time) { should == output_date }
      its(:stop_time) { should == output_date.to_time + 1.hour }
    end

    context 'with unix timestamp pattern' do
      let(:pattern) { 'logs/%H-s.log' }
      let(:input_path) { 'logs/1365202800.log' }
      let(:output_date) { DateTime.civil(2013,04,05,23) }

      its(:path) { should == input_path }
      its(:start_time) { should == output_date }
      its(:stop_time) { should == output_date.to_time + 1.hour }
    end
  end

  describe '#unify_path' do
    let(:induced) { described_class.new(plan, name, type, 'table/y=%Y/m=%m/d=%d/h=%H') }

    subject do
      instance.unify_path(input_path, induced)
    end

    context 'when input_path fully matches basis pattern' do
      let(:input_path) { 'report/2013-01-02/00' }

      its(:path) { should == 'table/y=2013/m=01/d=02/h=00' }
    end

    context 'when input_path partially matches basis pattern' do
      let(:induced) { described_class.new(plan, name, type, 'table/%Y-%m') }

      let(:input_path) { 'report/2013-01-02/00' }
      its(:path) { should == 'table/2013-01' }
    end

    context 'when input_path does not match basis pattern' do
      let(:input_path) { 'report' }

      it { expect { subject }.to raise_error /Cannot unify/ }
    end
  end

  describe '#matches' do
    subject do
      instance.matches?(input)
    end

    context 'when input fully matches' do
      let(:input) { 'report/2013-01-02/00' }
      it { should be_true }
    end

    context 'when input under matches' do
      let(:input) { 'report/2013-01-02' }
      it { should be_false }
    end

    context 'when input over matches' do
      let(:pattern) { 'report/%Y-%m-%d' }
      let(:input) { 'report/2013-01-02/00' }
      it { should be_false }
    end

    context 'when input does not match' do
      let(:input) { 'report' }
      it { should be_false }
    end

    context 'with alternative hour' do
      let(:pattern) { 'requests/y=%Y/m=%-m/d=%-d/h=%-k' }
      let(:input) { 'requests/y=2013/m=5/d=1/h=1' }
      it { should be_true }
    end

    context 'with alternative hour' do
      let(:pattern) { 'requests/y=%Y/m=%-m/d=%-d/h=%-k' }
      let(:input) { 'requests/y=2013/m=4/d=30/h=20' }
      it { should be_true }
    end

    context 'with wildcard pattern' do
      let(:pattern) { 'request_logs/%Y%m%d*request.log' }
      let(:input) { 'request_logs/20130524.random.request.log' }
      it { should be_true }
    end

    context 'with unix timestamp pattern' do
      let(:pattern) { 'request_logs/%H-s.log' }
      let(:input) { 'request_logs/1374192000.log' }
      it { should be_true }
    end
  end

  describe '#generate' do
    context 'with a block' do
      let(:start_date) { DateTime.civil(2013,04,05,20) }
      let(:stop_date) { DateTime.civil(2013,04,05,20) }
      specify { expect { |b| instance.generate(start_date, stop_date, &b) }.to yield_control }
    end

    context 'without a block' do
      let(:start_date) { DateTime.civil(2013,04,05,20) }
      let(:stop_date) { DateTime.civil(2013,04,05,22) }

      subject(:elems) do
        instance.generate(start_date, stop_date)
      end

      it { elems.map(&:path).should == ['report/2013-04-05/20', 'report/2013-04-05/21', 'report/2013-04-05/22'] }
    end
  end

  describe '#time_step' do
    subject { instance.time_step }

    context '24 hour' do
      let(:pattern) { '%Y-%m-%d/%k' }
      it { should == :hours }
    end
    context '24 hour (condensed)' do
      let(:pattern) { '%Y-%m-%d/%-k' }
      it { should == :hours }
    end
    context '12 hour' do
      let(:pattern) { '%Y-%m-%d/%H' }
      it { should == :hours }
    end
    context '12 hour (condensed)' do
      let(:pattern) { '%Y-%m-%d/%-H' }
      it { should == :hours }
    end
    context 'daily' do
      let(:pattern) { '%Y-%m-%d' }
      it { should == :days }
    end
    context 'monthly' do
      let(:pattern) { '%Y-%m' }
      it { should == :months }
    end
    context 'yearly' do
      let(:pattern) { '%Y' }
      it { should == :years }
    end
    context 'hourly unix' do
      let(:pattern) { '%H-s' }
      it { should == :hours }
    end
    context 'daily unix' do
      let(:pattern) { '%d-s' }
      it { should == :days }
    end
    context 'monthly unix' do
      let(:pattern) { '%m-s' }
      it { should == :months }
    end
    context 'yearly unix' do
      let(:pattern) { '%Y-s' }
      it { should == :years }
    end
  end

  describe '#time_round' do
    let(:input_time) { DateTime.civil(2013,9,13,23,13) }
    subject { instance.time_round(input_time) }

    before do
      instance.stub(:time_step) { time_step }
    end

    context 'hourly' do
      let(:time_step) { :hours }
      it { should == DateTime.civil(2013,9,13,23) }
    end
    context 'daily' do
      let(:time_step) { :days }
      it { should == DateTime.civil(2013,9,13) }
    end
    context 'monthly' do
      let(:time_step) { :months }
      it { should == DateTime.civil(2013,9) }
    end
    context 'yearly' do
      let(:time_step) { :years }
      it { should == DateTime.civil(2013) }
    end
  end

  describe '#round' do
    subject(:new_instance) { instance.round(grain) }

    context 'with totally partitioned pattern' do
      let(:pattern) { 'table/y=%Y/m=%m/d=%d/h=%H' }
      context 'with :hour' do
        let(:grain) { :hour }
        its(:pattern) { should == 'table/y=%Y/m=%m/d=%d/h=%H' }
      end

      context 'with :day' do
        let(:grain) { :day }
        its(:pattern) { should == 'table/y=%Y/m=%m/d=%d' }
      end

      context 'with :month' do
        let(:grain) { :month }
        its(:pattern) { should == 'table/y=%Y/m=%m' }
      end

      context 'with :year' do
        let(:grain) { :year }
        its(:pattern) { should == 'table/y=%Y' }
      end
    end

    context 'with partially partitioned pattern' do
      let(:pattern) { 'table/%Y-%m-%d/%H' }

      context 'with :hour' do
        let(:grain) { :hour }
        its(:pattern) { should == 'table/%Y-%m-%d/%H' }
      end

      context 'with :day' do
        let(:grain) { :day }
        its(:pattern) { should == 'table/%Y-%m-%d' }
      end

      context 'with :month' do
        let(:grain) { :month }
        its(:pattern) { should == 'table/%Y-%m-%d' }
      end

      context 'with :year' do
        let(:grain) { :year }
        its(:pattern) { should == 'table/%Y-%m-%d' }
      end
    end

    context 'when cannot round due to granularity' do
      shared_context 'cannot round' do
        it { expect { subject }.to raise_error RuntimeError, /cannot round to :#{grain} for #{pattern}/ }
      end

      context 'with :hour' do
        let(:grain) { :hour }
        let(:pattern) { 'table/y=%Y/m=%m/d=%d' }
        include_context 'cannot round'
      end

      context 'with :day' do
        let(:grain) { :day }
        let(:pattern) { 'table/y=%Y/m=%m' }
        include_context 'cannot round'
      end

      context 'with :month' do
        let(:grain) { :month }
        let(:pattern) { 'table/y=%Y' }
        include_context 'cannot round'
      end
    end
  end
end
