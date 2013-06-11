require 'spec_helper'

describe Masamune::DataPlanRule do
  let(:pattern) { 'report/%Y-%m-%d/%H' }
  let(:options) { {} }

  let(:instance) { described_class.new(pattern, options) }

  describe '#bind_date' do
    subject do
      instance.bind_date(input_date)
    end

    context 'with default' do
      let(:input_date) { DateTime.civil(2013,04,05,23,13) }

      its(:path) { should == 'report/2013-04-05/23' }
      its(:start_time) { should == input_date }
      its(:stop_time) { should == input_date.to_time + 1.hour }
    end
  end

  describe '#bind_path' do
    subject do
      instance.bind_path(input_path)
    end

    context 'with default' do
      let(:input_path) { 'report/2013-04-05/23' }
      let(:input_date) { DateTime.civil(2013,04,05,23) }

      its(:path) { should == input_path }
      its(:start_time) { should == input_date }
      its(:stop_time) { should == input_date.to_time + 1.hour }
    end
  end

  describe '#unify_path' do
    let(:induced) { described_class.new('table/y=%Y/m=%m/d=%d/h=%H') }

    subject do
      instance.unify_path(input_path, induced)
    end

    context 'when input_path fully matches basis pattern' do
      let(:input_path) { 'report/2013-01-02/00' }

      its(:path) { should == 'table/y=2013/m=01/d=02/h=00' }
    end

    context 'when input_path partially matches basis pattern' do
      let(:induced) { described_class.new('table/%Y-%m') }

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
  end
end
