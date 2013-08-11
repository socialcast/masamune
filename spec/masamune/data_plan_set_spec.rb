require 'spec_helper'

describe Masamune::DataPlanSet do
  let(:fs) { Masamune::MockFilesystem.new }
  before do
    Masamune.configure do |config|
      config.filesystem = fs
    end
  end

  let!(:plan) { Masamune::DataPlan.new }
  let!(:source_rule) { plan.add_source_rule('primary', 'log/%Y%m%d.*.log') }
  let!(:target_rule) { plan.add_target_rule('primary', 'table/y=%Y/m=%m/d=%d') }

  describe '#missing' do
    let(:paths) { ['table/y=2013/m=01/d=01', 'table/y=2013/m=01/d=02', 'table/y=2013/m=01/d=03'] }

    let(:instance) { Masamune::DataPlanSet.new(target_rule, paths) }

    subject(:missing) do
      instance.missing
    end

    context 'when all missing' do
      it { missing.should have(3).items }
      it { missing.should include 'table/y=2013/m=01/d=01' }
      it { missing.should include 'table/y=2013/m=01/d=02' }
      it { missing.should include 'table/y=2013/m=01/d=03' }
    end

    context 'when some missing' do
      before do
        fs.touch!('table/y=2013/m=01/d=01', 'table/y=2013/m=01/d=02')
      end
      it { missing.should have(1).items }
      it { missing.should include 'table/y=2013/m=01/d=03' }
    end

    context 'when none missing' do
      before do
        fs.touch!('table/y=2013/m=01/d=01', 'table/y=2013/m=01/d=02', 'table/y=2013/m=01/d=03')
      end
      it { missing.should be_empty }
    end
  end

  describe '#existing' do
    let(:instance) { Masamune::DataPlanSet.new(source_rule, paths) }

    subject(:existing) do
      instance.existing
    end

    context 'with basic paths' do
      let(:paths) { ['log/20130101.random_1.log', 'log/20130102.random_1.log'] }

      context 'when none existing' do
        it { existing.should be_empty }
      end

      context 'when some existing' do
        before do
          fs.touch!('log/20130101.random_1.log', 'log/20130101.random_2.log')
        end
        it { existing.should have(1).items }
        it { existing.should include 'log/20130101.random_1.log' }
      end

      context 'when all existing' do
        before do
          fs.touch!('log/20130101.random_1.log', 'log/20130101.random_2.log')
          fs.touch!('log/20130102.random_1.log', 'log/20130102.random_2.log')
        end
        it { existing.should have(2).items }
        it { existing.should include 'log/20130101.random_1.log' }
        it { existing.should include 'log/20130102.random_1.log' }
      end
    end

    context 'with wildcard paths' do
      let(:paths) { ['log/20130101.*.log', 'log/20130102.*.log'] }

      context 'when none existing' do
        it { existing.should be_empty }
      end

      context 'when some existing' do
        before do
          fs.touch!('log/20130101.random_1.log', 'log/20130101.random_2.log')
        end
        it { existing.should have(2).items }
        it { existing.should include 'log/20130101.random_1.log' }
        it { existing.should include 'log/20130101.random_2.log' }
      end

      context 'when all existing' do
        before do
          fs.touch!('log/20130101.random_1.log', 'log/20130101.random_2.log')
          fs.touch!('log/20130102.random_1.log', 'log/20130102.random_2.log')
        end
        it { existing.should have(4).items }
        it { existing.should include 'log/20130101.random_1.log' }
        it { existing.should include 'log/20130101.random_2.log' }
        it { existing.should include 'log/20130102.random_1.log' }
        it { existing.should include 'log/20130102.random_2.log' }
      end
    end
  end

  describe '#adjacent' do
    let(:instance) { Masamune::DataPlanSet.new(source_rule, paths) }

    subject(:sources) do
      instance.adjacent
    end

    subject(:existing) do
      instance.adjacent.existing
    end

    context 'with window of 1 time_step' do
      let(:paths) { ['log/20130101.*.log'] }

      before do
        instance.rule.stub(:window) { 1}
      end

      it { sources.should have(3).items }
      it { sources.should include 'log/20121231.*.log' }
      it { sources.should include 'log/20130101.*.log' }
      it { sources.should include 'log/20130102.*.log' }

      context 'when none existing' do
        it { existing.should be_empty }
      end

      context 'when some existing' do
        before do
          fs.touch!('log/20130101.random_1.log', 'log/20130101.random_2.log')
        end
        it { existing.should have(2).items }
        it { existing.should include 'log/20130101.random_1.log' }
        it { existing.should include 'log/20130101.random_2.log' }
      end

      context 'when all existing' do
        before do
          fs.touch!('log/20121231.random_1.log', 'log/20121231.random_2.log')
          fs.touch!('log/20130101.random_1.log', 'log/20130101.random_2.log')
          fs.touch!('log/20130102.random_1.log', 'log/20130102.random_2.log')
          fs.touch!('log/20130103.random_1.log', 'log/20130103.random_2.log')
        end
        it { existing.should have(6).items }
        it { existing.should include 'log/20121231.random_1.log' }
        it { existing.should include 'log/20121231.random_2.log' }
        it { existing.should include 'log/20130101.random_1.log' }
        it { existing.should include 'log/20130101.random_2.log' }
        it { existing.should include 'log/20130102.random_1.log' }
        it { existing.should include 'log/20130102.random_2.log' }
      end
    end
  end

  describe '#actionable' do
    let(:paths) { ['table/y=2013/m=01/d=01', 'table/y=2013/m=01/d=02', 'table/y=2013/m=01/d=03'] }

    let(:instance) { Masamune::DataPlanSet.new(target_rule, paths) }

    subject(:actionable) do
      instance.actionable
    end

    context 'when all sources missing' do
      it { actionable.should be_empty }
    end

    context 'when some sources missing' do
      before do
        fs.touch!('log/20130101.random_1.log', 'log/20130101.random_2.log')
      end
      it { actionable.should have(1).items }
      it { actionable.should include 'table/y=2013/m=01/d=01' }
    end

    context 'when all sources existing' do
      before do
        fs.touch!('log/20130101.random_1.log', 'log/20130101.random_2.log')
        fs.touch!('log/20130102.random_1.log', 'log/20130102.random_2.log')
        fs.touch!('log/20130103.random_1.log', 'log/20130103.random_2.log')
      end
      it { actionable.should have(3).items }
      it { actionable.should include 'table/y=2013/m=01/d=01' }
      it { actionable.should include 'table/y=2013/m=01/d=02' }
      it { actionable.should include 'table/y=2013/m=01/d=03' }
    end
  end


  describe '#include?' do
    let(:instance) { Masamune::DataPlanSet.new(source_rule, enum) }
    subject do
      instance.include?(elem)
    end

    context 'with basic enum and basic elem' do
      let(:enum) { ['log/20130101.random_1.log', 'log/20130102.random_2.log'] }
      let(:elem) { 'log/20130101.random_1.log' }

      it { should be_true }
    end

    context 'with basic enum and wildcard elem' do
      let(:enum) { ['log/20130101.random_1.log', 'log/20130102.random_2.log'] }
      let(:elem) { 'log/20130101.*.log' }

      it { should be_false }
    end

    context 'with wildcard enum and wildcard elem' do
      let(:enum) { ['log/20130101.*.log', 'log/20130102.*.log'] }
      let(:elem) { 'log/20130101.*.log' }

      it { should be_true }
    end

    context 'with wildcard enum and basic elem' do
      let(:enum) { ['log/20130101.*.log', 'log/20130102.*.log'] }
      let(:elem) { 'log/20130101.random_1.log' }

      it { should be_false  }
    end
  end
end
