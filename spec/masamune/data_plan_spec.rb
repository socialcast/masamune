require 'spec_helper'

describe Masamune::DataPlan do
  let(:fs) { MockFilesystem.new }
  before do
    Masamune.configure do |config|
      config.filesystem = fs
    end
  end

  let(:plan) { Masamune::DataPlan.new }
  let(:primary_command) { Proc.new { } }
  let(:derived_daily_command) { Proc.new {} }
  let(:derived_monthly_command) { Proc.new {} }
  let(:primary_options) { {:wildcard => true} }

  before do
    plan.add_target('primary', 'table/y=%Y/m=%m/d=%d')
    plan.add_source('primary', 'log/%Y%m%d.*.log', primary_options)
    plan.add_command('primary', primary_command)
    plan.add_target('derived_daily', 'daily/%Y-%m-%d')
    plan.add_source('derived_daily', 'table/y=%Y/m=%m/d=%d')
    plan.add_command('derived_daily', derived_daily_command)
    plan.add_target('derived_monthly', 'monthly/%Y-%m')
    plan.add_source('derived_monthly', 'table/y=%Y/m=%m/d=%d')
    plan.add_command('derived_monthly', derived_monthly_command)
  end

  describe '#sources_from_paths' do
    let(:rule) { 'primary' }
    let(:paths) { ['log/20130101.random.log', 'log/20130102.random.log'] }

    subject { plan.sources_from_paths(rule, paths).map(&:path) }

    it { should == [
      'log/20130101.random.log',
      'log/20130102.random.log' ] }

    context 'with window of 1 time_step' do
      let(:primary_options) { {:wildcard => true, :window => 1} }

      it { should == [
        'log/20121231.random.log',
        'log/20130101.random.log',
        'log/20130102.random.log',
        'log/20130103.random.log' ] }
    end

    context 'with window of 3 time_steps' do
      let(:primary_options) { {:wildcard => true, :window => 3} }

      it { should == [
        'log/20121229.random.log',
        'log/20121230.random.log',
        'log/20121231.random.log',
        'log/20130101.random.log',
        'log/20130102.random.log',
        'log/20130103.random.log',
        'log/20130104.random.log',
        'log/20130105.random.log' ] }
    end
  end

  describe '#targets_for_date_range' do
    let(:start) { Date.civil(2013,01,01) }
    let(:stop) { Date.civil(2013,01,03) }

    subject { plan.targets_for_date_range(rule, start, stop).map(&:path) }

    context 'primary' do
      let(:rule) { 'primary' }
      it { should include 'table/y=2013/m=01/d=01' }
      it { should include 'table/y=2013/m=01/d=02' }
      it { should include 'table/y=2013/m=01/d=03' }
      it { should have(3).items }
    end

    context 'derived_daily' do
      let(:rule) { 'derived_daily' }
      it { should include 'daily/2013-01-01' }
      it { should include 'daily/2013-01-02' }
      it { should include 'daily/2013-01-03' }
      it { should have(3).items }
    end

    context 'derived_monthly' do
      let(:rule) { 'derived_monthly' }
      it { should include 'monthly/2013-01' }
      it { should have(1).items }
    end
  end

  describe '#targets_for_source' do
    subject(:targets) do
      plan.targets_for_source(rule, source)
    end

    context 'primary' do
      let(:rule) { 'primary' }
      let(:source) { 'log/20130101.random.log' }

      it { targets.first.start_time.should == Date.civil(2013,01,01) }
      it { targets.first.stop_time.should == Date.civil(2013,01,02) }
      it { targets.first.path.should == 'table/y=2013/m=01/d=01' }
    end

    context 'derived_daily' do
      let(:rule) { 'derived_daily' }
      let(:source)  { 'table/y=2013/m=01/d=01' }

      it { targets.first.start_time.should == Date.civil(2013,01,01) }
      it { targets.first.stop_time.should == Date.civil(2013,01,02) }
      it { targets.first.path.should == 'daily/2013-01-01' }
    end

    context 'derived_monthly' do
      let(:rule) { 'derived_monthly' }
      let(:source)  { 'table/y=2013/m=01/d=01' }

      it { targets.first.start_time.should == Date.civil(2013,01,01) }
      it { targets.first.stop_time.should == Date.civil(2013,02,01) }
      it { targets.first.path.should == 'monthly/2013-01' }
    end
  end

  describe '#sources_for_target' do
    subject(:sources) do
      plan.sources_for_target(rule, target)
    end

    before do
      fs.touch!('log/20130101.app1.log')
      fs.touch!('log/20130101.app2.log')
      fs.touch!('log/20130104.app1.log')
      fs.touch!('log/20130104.app2.log')
    end

    context 'valid target associated with wildcard source' do
      let(:rule) { 'primary' }
      let(:target) { 'table/y=2013/m=01/d=01' }

      it { sources.first.path.should == 'log/20130101.app1.log' }
      it { sources.last.path.should == 'log/20130101.app2.log' }
    end

    context 'valid target associated with a single source file' do
      let(:rule) { 'derived_daily' }
      let(:target) { 'daily/2013-01-03' }

      it { sources.first.path.should == 'table/y=2013/m=01/d=03' }
    end

    context 'valid target associated with a group of source files' do
      let(:rule) { 'derived_monthly' }
      let(:target) { 'monthly/2013-01' }

      (1..31).each do |day|
        it { sources.map(&:path).should include 'table/y=2013/m=01/d=%02d' % day }
      end
      it { sources.should have(31).items }
    end

    context 'invalid target' do
      let(:rule) { 'derived_daily' }
      let(:target) {  'table/y=2013/m=01/d=01' }
      it { expect { subject }.to raise_error }
    end

    context 'with window of 1 time_step' do
      let(:rule) { 'primary' }
      let(:target) { 'table/y=2013/m=01/d=01' }
      let(:primary_options) { {:wildcard => true, :window => 1} }

      it { sources.map(&:path).should == [
        'log/20121231.app1.log',
        'log/20130101.app1.log',
        'log/20130102.app1.log',
        'log/20121231.app2.log',
        'log/20130101.app2.log',
        'log/20130102.app2.log'] }
    end
  end

  describe '#rule_for_target' do
    subject { plan.rule_for_target(target) }

    context 'primary target' do
      let(:target) { 'table/y=2013/m=01/d=01' }
      it { should == 'primary' }
    end

    context 'derived_daily target' do
      let(:target) { 'daily/2013-01-03' }
      it { should == 'derived_daily' }
    end

    context 'derived_monthly target' do
      let(:target) { 'monthly/2013-01' }
      it { should == 'derived_monthly' }
    end

    context 'invalid target' do
      let(:target) { 'daily' }
      it { expect { subject }.to raise_error }
    end
  end

  describe '#resolve' do
    subject(:resolve) { plan.resolve(rule, targets) }

    context 'primary rule' do
      let(:rule) { 'primary' }
      let(:targets) {  [
        'table/y=2013/m=01/d=01',
        'table/y=2013/m=01/d=02',
        'table/y=2013/m=01/d=03' ] }

      context 'when target data exists' do
        before do
          fs.touch!('table/y=2013/m=01/d=01')
          fs.touch!('table/y=2013/m=01/d=02')
          fs.touch!('table/y=2013/m=01/d=03')
          primary_command.should_not_receive(:call)
          derived_daily_command.should_not_receive(:call)
          resolve
        end

        it { should be_false }
        it 'should not call primary_command' do; end
        it 'should not call derived_daily_command' do; end
      end

      context 'when partial target data exists' do
        before do
          fs.touch!('log/20130101.app1.log')
          fs.touch!('log/20130102.app1.log')
          fs.touch!('log/20130103.app1.log')
          fs.touch!('table/y=2013/m=01/d=01')
          fs.touch!('table/y=2013/m=01/d=03')
          primary_command.should_receive(:call).with(['log/20130102.app1.log'], {})
          derived_daily_command.should_not_receive(:call)
          resolve
        end

        it { should be_true }
        it 'should call primary_command' do; end
        it 'should not call derived_daily_command' do; end
      end

      context 'when source data does not exist' do
        before do
          primary_command.should_not_receive(:call)
          derived_daily_command.should_not_receive(:call)
          resolve
        end

        it { should be_false }
        it 'should not call primary_command' do; end
        it 'should not call derived_daily_command' do; end
      end
    end

    shared_examples_for 'derived daily data' do
      let(:primary_command) {
        Proc.new do
          fs.touch!('table/y=2013/m=01/d=01')
          fs.touch!('table/y=2013/m=01/d=02')
          fs.touch!('table/y=2013/m=01/d=03')
        end
      }

      context 'when primary target data exists' do
        before do
          fs.touch!('log/20130101.app1.log')
          fs.touch!('log/20130102.app1.log')
          fs.touch!('log/20130103.app1.log')
          primary_command.should_receive(:call).with(['log/20130101.app1.log', 'log/20130102.app1.log', 'log/20130103.app1.log'], {}).and_call_original
          derived_command.should_receive(:call).with(["table/y=2013/m=01/d=01", "table/y=2013/m=01/d=02", "table/y=2013/m=01/d=03"], {})
          resolve
        end

        it { should be_true }
        it 'should call primary_command' do; end
        it 'should not call derived_command' do; end
      end
    end

    context 'derived_daily rule' do
      let(:rule) { 'derived_daily' }
      let(:targets) {  [
        'daily/2013-01-01',
        'daily/2013-01-02',
        'daily/2013-01-03' ] }

      it_behaves_like 'derived daily data' do
        let(:derived_command) { derived_daily_command }
      end
    end

    context 'derived_monthly rule' do
      let(:rule) { 'derived_monthly' }
      let(:targets) {  ['monthly/2013-01'] }

      it_behaves_like 'derived daily data' do
        let(:derived_command) { derived_monthly_command }
      end
    end
  end
end
