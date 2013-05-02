require 'spec_helper'

describe Fixpoint::DataPlan do
  let(:fs) { MockFilesystem.new }
  let(:handler) { Proc.new {} }
  let(:plan) { Fixpoint::DataPlan.new(fs, handler) }

  before do
    # FIXME multi-level dependancy resolution
=begin
    plan.add_rule('table/y=%Y/m=%m/d=%d', 'incoming/%Y-%m') do |match|
      match.targets.each do |target|
        fs.touch! target
      end
    end
=end

    plan.add_rule('report/%Y-%m-%d', 'table/y=%Y/m=%m/d=%d') do |match|
      match.targets.each do |target|
        fs.touch! target
      end
    end
  end

  describe '#resolve' do
    let(:start) { Date.civil(2013,01,01) }
    let(:stop) { Date.civil(2013,01,03) }

    context 'when partial target data exists' do
      before do
        fs.touch!('report/2013-01-01')
        fs.touch!('table/y=2013/m=01/d=01')
        fs.touch!('table/y=2013/m=01/d=02')
        fs.touch!('table/y=2013/m=01/d=03')
        fs.should_receive(:touch!).with('report/2013-01-01').never
        fs.should_receive(:touch!).with('report/2013-01-02').once
        fs.should_receive(:touch!).with('report/2013-01-03').once
        handler.should_receive(:call).never
        plan.resolve(start, stop)
      end

      it 'touches missing target' do; end
    end

    context 'when full target data exists' do
      before do
        fs.touch!('report/2013-01-01')
        fs.touch!('report/2013-01-02')
        fs.touch!('report/2013-01-03')
        fs.should_receive(:touch!).with('report/2013-01-01').never
        fs.should_receive(:touch!).with('report/2013-01-02').never
        fs.should_receive(:touch!).with('report/2013-01-03').never
        handler.should_receive(:call).never
        plan.resolve(start, stop)
      end

      it 'does touch existing data' do; end
    end

    context 'when source data is missing' do
      before do
        fs.touch!('report/2013-01-01')
        fs.should_receive(:touch!).with('report/2013-01-01').never
        fs.should_receive(:touch!).with('report/2013-01-02').never
        fs.should_receive(:touch!).with('report/2013-01-03').never
        handler.should_receive(:call).twice
        plan.resolve(start, stop)
      end

      it 'skips target for missing source data and signals handler' do; end
    end
  end
end
