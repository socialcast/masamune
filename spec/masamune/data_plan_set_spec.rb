require 'spec_helper'

describe Masamune::DataPlanSet do
  let(:fs) { Masamune::MockFilesystem.new }
  before do
    Masamune.configure do |config|
      config.filesystem = fs
    end
  end

  let(:plan) { Masamune::DataPlan.new }
  let(:instance) { Masamune::DataPlanSet.new(set) }

  before do
    plan.add_target_rule('primary', 'table/y=%Y/m=%m/d=%d')
    plan.add_source_rule('primary', 'log/%Y%m%d.*.log')
  end

  describe '#missing' do
    let(:paths) { ['table/y=2013/m=01/d=01', 'table/y=2013/m=01/d=02', 'table/y=2013/m=01/d=03'] }

    let(:set) { plan.targets_from_paths2('primary', paths) }

    subject(:missing) do
      instance.missing
    end

    context 'when all missing' do
      it { missing.map(&:path).should == paths }
    end

    context 'when some missing' do
      before do
        fs.touch!('table/y=2013/m=01/d=01', 'table/y=2013/m=01/d=02')
      end
      it { missing.map(&:path).should == ['table/y=2013/m=01/d=03'] }
    end

    context 'when none missing' do
      before do
        fs.touch!('table/y=2013/m=01/d=01', 'table/y=2013/m=01/d=02', 'table/y=2013/m=01/d=03')
      end
      it { should be_empty }
    end
  end
end
