require 'spec_helper'

describe Masamune::DataPlanSet do
  let(:fs) { Masamune::MockFilesystem.new }
  let!(:plan) { Masamune::DataPlan.new }

  let!(:source_rule) { plan.add_source_rule('primary', path: 'log/%Y%m%d.*.log') }
  let!(:target_rule) { plan.add_target_rule('primary', path: 'table/y=%Y/m=%m/d=%d') }

  before do
    plan.filesystem = fs
  end

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
        instance.rule.stub(:window) { 1 }
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

    subject(:actionable_targets) do
      instance.actionable
    end

    subject(:actionable_sources) do
      instance.actionable.sources
    end

    context 'when all sources missing' do
      it { actionable_targets.should be_empty }
      it { actionable_sources.should be_empty }
    end

    context 'when some sources missing' do
      before do
        fs.touch!('log/20130101.random_1.log', 'log/20130101.random_2.log')
      end
      it { actionable_targets.should have(1).items }
      it { actionable_targets.should include 'table/y=2013/m=01/d=01' }
      it { actionable_sources.should have(1).items }
      it { actionable_sources.should include 'log/20130101.*.log' }
    end

    context 'when all sources existing' do
      before do
        fs.touch!('log/20130101.random_1.log', 'log/20130101.random_2.log')
        fs.touch!('log/20130102.random_1.log', 'log/20130102.random_2.log')
        fs.touch!('log/20130103.random_1.log', 'log/20130103.random_2.log')
      end
      it { actionable_targets.should have(3).items }
      it { actionable_targets.should include 'table/y=2013/m=01/d=01' }
      it { actionable_targets.should include 'table/y=2013/m=01/d=02' }
      it { actionable_targets.should include 'table/y=2013/m=01/d=03' }
      it { actionable_sources.should have(3).items }
      it { actionable_sources.should include 'log/20130101.*.log' }
      it { actionable_sources.should include 'log/20130102.*.log' }
      it { actionable_sources.should include 'log/20130103.*.log' }
    end
  end

  describe '#stale' do
    context 'when source rule' do
      let(:paths) { ['log/20130101.random_1.log', 'log/20130102.random_1.log'] }
      let(:instance) { Masamune::DataPlanSet.new(source_rule, paths) }

      subject(:stale_sources) do
        instance.stale
      end

      it { stale_sources.should be_empty }
    end

    let(:paths) { ['table/y=2013/m=01/d=01', 'table/y=2013/m=01/d=02', 'table/y=2013/m=01/d=03'] }
    let(:instance) { Masamune::DataPlanSet.new(target_rule, paths) }
    let(:prev_time) { Time.parse('2013-01-01 09:00:00 +0000') }
    let(:next_time) { Time.parse('2013-01-01 10:00:00 +0000') }

    before do
      fs.touch!('log/20130101.random_1.log', 'log/20130101.random_2.log', mtime: prev_time)
      fs.touch!('log/20130102.random_1.log', 'log/20130102.random_2.log', mtime: prev_time)
      fs.touch!('log/20130103.random_1.log', 'log/20130103.random_2.log', mtime: prev_time)
      fs.touch!('table/y=2013/m=01/d=01', 'table/y=2013/m=01/d=02', 'table/y=2013/m=01/d=03', mtime: prev_time)
    end

    subject(:stale_targets) do
      instance.stale
    end

    context 'when none stale targets' do
      it { stale_targets.should be_empty }
    end

    context 'when some stale targets (first source)' do
      before do
        fs.touch!('log/20130101.random_1.log', mtime: next_time)
      end

      it { stale_targets.should have(1).items }
      it { stale_targets.should include 'table/y=2013/m=01/d=01' }
    end

    context 'when some stale targets (second source)' do
      before do
        fs.touch!('log/20130101.random_2.log', mtime: next_time)
      end

      it { stale_targets.should have(1).items }
      it { stale_targets.should include 'table/y=2013/m=01/d=01' }
    end

    context 'when all stale targets' do
      before do
        fs.touch!('log/20130101.random_1.log', mtime: next_time)
        fs.touch!('log/20130102.random_2.log', mtime: next_time)
        fs.touch!('log/20130103.random_1.log', mtime: next_time)
        fs.touch!('log/20130103.random_2.log', mtime: next_time)
      end

      it { stale_targets.should have(3).items }
      it { stale_targets.should include 'table/y=2013/m=01/d=01' }
      it { stale_targets.should include 'table/y=2013/m=01/d=02' }
      it { stale_targets.should include 'table/y=2013/m=01/d=03' }
    end

    context 'when missing source last_modified_at' do
      before do
        fs.touch!('log/20130101.random_1.log', mtime: Masamune::DataPlanElem::MISSING_MODIFIED_AT)
      end

      it { stale_targets.should be_empty }
    end
  end

  describe '#with_grain' do
    let(:paths) { ['table/y=2012/m=12/d=29', 'table/y=2012/m=12/d=30', 'table/y=2012/m=12/d=31',
                   'table/y=2013/m=01/d=01', 'table/y=2013/m=01/d=02', 'table/y=2013/m=02/d=01', ] }

    let(:instance) { Masamune::DataPlanSet.new(target_rule, paths) }

    subject(:granular_targets) do
      instance.with_grain(grain)
    end

    context 'when :day' do
      let(:grain) { :day }
      it { should have(6).items }
      it { should include 'table/y=2012/m=12/d=29' }
      it { should include 'table/y=2012/m=12/d=30' }
      it { should include 'table/y=2012/m=12/d=31' }
      it { should include 'table/y=2013/m=01/d=01' }
      it { should include 'table/y=2013/m=01/d=02' }
      it { should include 'table/y=2013/m=02/d=01' }
    end

    context 'when :month' do
      let(:grain) { :month }
      it { should have(3).items }
      it { should include 'table/y=2012/m=12' }
      it { should include 'table/y=2013/m=01' }
      it { should include 'table/y=2013/m=02' }
    end

    context 'when :year' do
      let(:grain) { :year }
      it { should have(2).items }
      it { should include 'table/y=2012' }
      it { should include 'table/y=2013' }
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
