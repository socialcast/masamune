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
  let(:secondary_command) { Proc.new {} }

  before do
    plan.add_target('primary', 'table/y=%Y/m=%m/d=%d')
    plan.add_source('primary', 'log/%Y%m%d.*.log', :wildcard => true)
    plan.add_command('primary', primary_command)
    plan.add_target('secondary', 'report/%Y-%m-%d')
    plan.add_source('secondary', 'table/y=%Y/m=%m/d=%d')
    plan.add_command('secondary', secondary_command)
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
    end

    context 'secondary' do
      let(:rule) { 'secondary' }
      it { should include 'report/2013-01-01' }
      it { should include 'report/2013-01-02' }
      it { should include 'report/2013-01-03' }
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

    context 'secondary' do
      let(:rule) { 'secondary' }
      let(:source)  { 'table/y=2013/m=01/d=01' }

      it { targets.first.start_time.should == Date.civil(2013,01,01) }
      it { targets.first.stop_time.should == Date.civil(2013,01,02) }
      it { targets.first.path.should == 'report/2013-01-01' }
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

    context 'valid target associated with source files' do
      let(:rule) { 'secondary' }
      let(:target) { 'report/2013-01-03' }

      it { sources.first.path.should == 'table/y=2013/m=01/d=03' }
    end

    context 'invalid target' do
      let(:rule) { 'secondary' }
      let(:target) {  'table/y=2013/m=01/d=01' }
      it { expect { subject }.to raise_error }
    end
  end

  describe '#rule_for_target' do
    subject { plan.rule_for_target(target) }

    context 'primary target' do
      let(:target) { 'table/y=2013/m=01/d=01' }
      it { should == 'primary' }
    end

    context 'secondary target' do
      let(:target) { 'report/2013-01-03' }
      it { should == 'secondary' }
    end

    context 'invalid target' do
      let(:target) { 'report' }
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
          secondary_command.should_not_receive(:call)
          resolve
        end

        it { should be_false }
        it 'should not call primary_command' do; end
        it 'should not call secondary_command' do; end
      end

      context 'when partial target data exists' do
        before do
          fs.touch!('log/20130101.app1.log')
          fs.touch!('log/20130102.app1.log')
          fs.touch!('log/20130103.app1.log')
          fs.touch!('table/y=2013/m=01/d=01')
          fs.touch!('table/y=2013/m=01/d=03')
          primary_command.should_receive(:call).with(['log/20130102.app1.log'], {})
          secondary_command.should_not_receive(:call)
          resolve
        end

        it { should be_true }
        it 'should call primary_command' do; end
        it 'should not call secondary_command' do; end
      end

      context 'when source data does not exist' do
        before do
          primary_command.should_not_receive(:call)
          secondary_command.should_not_receive(:call)
          resolve
        end

        it { should be_false }
        it 'should not call primary_command' do; end
        it 'should not call secondary_command' do; end
      end
    end

    context 'secondary rule' do
      let(:rule) { 'secondary' }
      let(:targets) {  [
        'report/2013-01-01',
        'report/2013-01-02',
        'report/2013-01-03' ] }

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
          secondary_command.should_receive(:call).with(["table/y=2013/m=01/d=01", "table/y=2013/m=01/d=02", "table/y=2013/m=01/d=03"], {})
          resolve
        end

        it { should be_true }
        it 'should call primary_command' do; end
        it 'should not call secondary_command' do; end
      end
    end
  end
end
