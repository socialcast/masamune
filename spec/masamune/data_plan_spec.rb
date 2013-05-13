require 'spec_helper'

describe Masamune::DataPlan do
  let(:fs) { MockFilesystem.new }
  let(:plan) { Masamune::DataPlan.new }

  before do
    plan.add_rule('report/%Y-%m-%d', {}, 'table/y=%Y/m=%m/d=%d', {}, 'command') do |file|
      fs.exists? file
    end
  end

  describe '#resolve' do
    let(:start) { Date.civil(2013,01,01) }
    let(:stop) { Date.civil(2013,01,03) }

    subject { plan.matches['command'] }

    context 'when partial target data exists' do
      before do
        fs.touch!('report/2013-01-01')
        fs.touch!('table/y=2013/m=01/d=01')
        fs.touch!('table/y=2013/m=01/d=02')
        fs.touch!('table/y=2013/m=01/d=03')
        plan.resolve(start, stop)
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
        plan.resolve(start, stop)
      end

      it { should be_empty }
    end
  end
end
