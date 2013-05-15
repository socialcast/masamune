require 'spec_helper'

describe Masamune::DataPlan do
  let(:fs) { MockFilesystem.new }
  before do
    Masamune.configure do |config|
      config.filesystem = fs
    end
  end

  let(:plan) { Masamune::DataPlan.new }
  before do
    plan.add_rule('table/%Y-%m-%d', {}, 'log/%Y%m%d.*.log', {}, 'forward')
    plan.add_rule('report/%Y-%m-%d', {}, 'table/y=%Y/m=%m/d=%d', {}, 'backward')
  end

  describe '.rule_step' do
    subject { plan.class.rule_step(input) }
    context '24 hour' do
      let(:input) { '%Y-%m-%d/%k' }
      it { should == 1.hour.to_i }
    end
    context '12 hour' do
      let(:input) { '%Y-%m-%d/%H' }
      it { should == 1.hour.to_i }
    end
    context 'daily' do
      let(:input) { '%Y-%m-%d' }
      it { should == 1.day.to_i }
    end
    context 'default' do
      let(:input) { nil }
      it { should == 1.day.to_i }
    end
  end

  describe '#resolve' do
    let(:start) { Date.civil(2013,01,01) }
    let(:stop) { Date.civil(2013,01,03) }

    subject { plan.matches['backward'] }

    context 'when partial target data exists' do
      before do
        fs.touch!('report/2013-01-01')
        fs.touch!('table/y=2013/m=01/d=01')
        fs.touch!('table/y=2013/m=01/d=02')
        fs.touch!('table/y=2013/m=01/d=03')
        plan.resolve(start, stop, 'backward')
      end

      it { should include 'table/y=2013/m=01/d=02' }
      it { should include 'table/y=2013/m=01/d=03' }
      it { should_not include 'table/y=2013/m=01/d=01' }
    end

    context 'when full target data exists' do
      before do
        fs.touch!('report/2013-01-01')
        fs.touch!('report/2013-01-02')
        fs.touch!('report/2013-01-03')
        plan.resolve(start, stop, 'backward')
      end

      it { should be_empty }
    end

    context 'glob rules' do
      subject { plan.matches['forward'] }

      context 'when partial source data exists' do
        before do
          fs.touch!('log/20130101.app1.log')
          fs.touch!('log/20130101.app2.log')
          fs.touch!('log/20130104.app1.log')
          fs.touch!('log/20130104.app2.log')
          plan.resolve(start, stop, 'forward')
        end

        it { should include 'log/20130101.app1.log' }
        it { should include 'log/20130101.app2.log' }
        it { should_not include 'log/20130104.app1.log' }
        it { should_not include 'log/20130104.app2.log' }
      end
    end
  end
end
