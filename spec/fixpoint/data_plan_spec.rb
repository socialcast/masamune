require 'spec_helper'

describe Fixpoint::DataPlan do
  let(:file_system) { MockFileSystem.new }
  let(:handler) { Proc.new {} }
  let(:plan) { Fixpoint::DataPlan.new(file_system, handler) }

  before do
    # FIXME multi-level dependancy resolution
=begin
    plan.add_rule('table/y=%Y/m=%m/d=%d', 'incoming/%Y-%m') do |match|
      match.targets.each do |target|
        file_system.touch! target
      end
    end
=end

    plan.add_rule('report/%Y-%m-%d', 'table/y=%Y/m=%m/d=%d') do |match|
      match.targets.each do |target|
        file_system.touch! target
      end
    end
  end

  describe '#resolve' do
    let(:start) { Date.civil(2013,01,01) }
    let(:stop) { Date.civil(2013,01,03) }

    context 'when partial target data exists' do
      before do
        file_system.touch!('report/2013-01-01')
        file_system.touch!('table/y=2013/m=01/d=01')
        file_system.touch!('table/y=2013/m=01/d=02')
        file_system.touch!('table/y=2013/m=01/d=03')
        file_system.should_receive(:touch!).with('report/2013-01-01').never
        file_system.should_receive(:touch!).with('report/2013-01-02').once
        file_system.should_receive(:touch!).with('report/2013-01-03').once
        handler.should_receive(:call).never
        plan.resolve(start, stop)
      end

      it 'touches missing target' do; end
    end

    context 'when full target data exists' do
      before do
        file_system.touch!('report/2013-01-01')
        file_system.touch!('report/2013-01-02')
        file_system.touch!('report/2013-01-03')
        file_system.should_receive(:touch!).with('report/2013-01-01').never
        file_system.should_receive(:touch!).with('report/2013-01-02').never
        file_system.should_receive(:touch!).with('report/2013-01-03').never
        handler.should_receive(:call).never
        plan.resolve(start, stop)
      end

      it 'does touch existing data' do; end
    end

    context 'when source data is missing' do
      before do
        file_system.touch!('report/2013-01-01')
        file_system.should_receive(:touch!).with('report/2013-01-01').never
        file_system.should_receive(:touch!).with('report/2013-01-02').never
        file_system.should_receive(:touch!).with('report/2013-01-03').never
        handler.should_receive(:call).twice
        plan.resolve(start, stop)
      end

      it 'skips target for missing source data and signals handler' do; end
    end
  end
end
