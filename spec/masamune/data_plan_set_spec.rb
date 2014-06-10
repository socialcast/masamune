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
      it { expect(missing.size).to eq(3) }
      it { expect(missing).to include 'table/y=2013/m=01/d=01' }
      it { expect(missing).to include 'table/y=2013/m=01/d=02' }
      it { expect(missing).to include 'table/y=2013/m=01/d=03' }
    end

    context 'when some missing' do
      before do
        fs.touch!('table/y=2013/m=01/d=01', 'table/y=2013/m=01/d=02')
      end
      it { expect(missing.size).to eq(1) }
      it { expect(missing).to include 'table/y=2013/m=01/d=03' }
    end

    context 'when none missing' do
      before do
        fs.touch!('table/y=2013/m=01/d=01', 'table/y=2013/m=01/d=02', 'table/y=2013/m=01/d=03')
      end
      it { expect(missing).to be_empty }
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
        it { expect(existing).to be_empty }
      end

      context 'when some existing' do
        before do
          fs.touch!('log/20130101.random_1.log', 'log/20130101.random_2.log')
        end
        it { expect(existing.size).to eq(1) }
        it { expect(existing).to include 'log/20130101.random_1.log' }
      end

      context 'when all existing' do
        before do
          fs.touch!('log/20130101.random_1.log', 'log/20130101.random_2.log')
          fs.touch!('log/20130102.random_1.log', 'log/20130102.random_2.log')
        end
        it { expect(existing.size).to eq(2) }
        it { expect(existing).to include 'log/20130101.random_1.log' }
        it { expect(existing).to include 'log/20130102.random_1.log' }
      end
    end

    context 'with wildcard paths' do
      let(:paths) { ['log/20130101.*.log', 'log/20130102.*.log'] }

      context 'when none existing' do
        it { expect(existing).to be_empty }
      end

      context 'when some existing' do
        before do
          fs.touch!('log/20130101.random_1.log', 'log/20130101.random_2.log')
        end
        it { expect(existing.size).to eq(2) }
        it { expect(existing).to include 'log/20130101.random_1.log' }
        it { expect(existing).to include 'log/20130101.random_2.log' }
      end

      context 'when all existing' do
        before do
          fs.touch!('log/20130101.random_1.log', 'log/20130101.random_2.log')
          fs.touch!('log/20130102.random_1.log', 'log/20130102.random_2.log')
        end
        it { expect(existing.size).to eq(4) }
        it { expect(existing).to include 'log/20130101.random_1.log' }
        it { expect(existing).to include 'log/20130101.random_2.log' }
        it { expect(existing).to include 'log/20130102.random_1.log' }
        it { expect(existing).to include 'log/20130102.random_2.log' }
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
        allow(instance.rule).to receive(:window) { 1 }
      end

      it { expect(sources.size).to eq(3) }
      it { expect(sources).to include 'log/20121231.*.log' }
      it { expect(sources).to include 'log/20130101.*.log' }
      it { expect(sources).to include 'log/20130102.*.log' }

      context 'when none existing' do
        it { expect(existing).to be_empty }
      end

      context 'when some existing' do
        before do
          fs.touch!('log/20130101.random_1.log', 'log/20130101.random_2.log')
        end
        it { expect(existing.size).to eq(2) }
        it { expect(existing).to include 'log/20130101.random_1.log' }
        it { expect(existing).to include 'log/20130101.random_2.log' }
      end

      context 'when all existing' do
        before do
          fs.touch!('log/20121231.random_1.log', 'log/20121231.random_2.log')
          fs.touch!('log/20130101.random_1.log', 'log/20130101.random_2.log')
          fs.touch!('log/20130102.random_1.log', 'log/20130102.random_2.log')
          fs.touch!('log/20130103.random_1.log', 'log/20130103.random_2.log')
        end
        it { expect(existing.size).to eq(6) }
        it { expect(existing).to include 'log/20121231.random_1.log' }
        it { expect(existing).to include 'log/20121231.random_2.log' }
        it { expect(existing).to include 'log/20130101.random_1.log' }
        it { expect(existing).to include 'log/20130101.random_2.log' }
        it { expect(existing).to include 'log/20130102.random_1.log' }
        it { expect(existing).to include 'log/20130102.random_2.log' }
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
      it { expect(actionable_targets).to be_empty }
      it { expect(actionable_sources).to be_empty }
    end

    context 'when some sources missing' do
      before do
        fs.touch!('log/20130101.random_1.log', 'log/20130101.random_2.log')
      end
      it { expect(actionable_targets.size).to eq(1) }
      it { expect(actionable_targets).to include 'table/y=2013/m=01/d=01' }
      it { expect(actionable_sources.size).to eq(1) }
      it { expect(actionable_sources).to include 'log/20130101.*.log' }
    end

    context 'when all sources existing' do
      before do
        fs.touch!('log/20130101.random_1.log', 'log/20130101.random_2.log')
        fs.touch!('log/20130102.random_1.log', 'log/20130102.random_2.log')
        fs.touch!('log/20130103.random_1.log', 'log/20130103.random_2.log')
      end
      it { expect(actionable_targets.size).to eq(3) }
      it { expect(actionable_targets).to include 'table/y=2013/m=01/d=01' }
      it { expect(actionable_targets).to include 'table/y=2013/m=01/d=02' }
      it { expect(actionable_targets).to include 'table/y=2013/m=01/d=03' }
      it { expect(actionable_sources.size).to eq(3) }
      it { expect(actionable_sources).to include 'log/20130101.*.log' }
      it { expect(actionable_sources).to include 'log/20130102.*.log' }
      it { expect(actionable_sources).to include 'log/20130103.*.log' }
    end
  end

  describe '#stale' do
    context 'when source rule' do
      let(:paths) { ['log/20130101.random_1.log', 'log/20130102.random_1.log'] }
      let(:instance) { Masamune::DataPlanSet.new(source_rule, paths) }

      subject(:stale_sources) do
        instance.stale
      end

      it { expect(stale_sources).to be_empty }
    end

    let(:paths) { ['table/y=2013/m=01/d=01', 'table/y=2013/m=01/d=02', 'table/y=2013/m=01/d=03'] }
    let(:instance) { Masamune::DataPlanSet.new(target_rule, paths) }
    let(:past_time) { Time.parse('2013-01-01 09:00:00 +0000') }
    let(:present_time) { Time.parse('2013-01-01 09:30:00 +0000') }
    let(:future_time) { Time.parse('2013-01-01 10:00:00 +0000') }

    before do
      fs.touch!('log/20130101.random_1.log', 'log/20130101.random_2.log', mtime: past_time)
      fs.touch!('log/20130102.random_1.log', 'log/20130102.random_2.log', mtime: past_time)
      fs.touch!('log/20130103.random_1.log', 'log/20130103.random_2.log', mtime: past_time)
      fs.touch!('table/y=2013/m=01/d=01', 'table/y=2013/m=01/d=02', 'table/y=2013/m=01/d=03', mtime: present_time)
    end

    subject(:stale_targets) do
      instance.stale
    end

    context 'when none stale targets' do
      it { expect(stale_targets).to be_empty }
    end

    context 'when some stale targets (first source)' do
      before do
        fs.touch!('log/20130101.random_1.log', mtime: future_time)
      end

      it { expect(stale_targets.size).to eq(1) }
      it { expect(stale_targets).to include 'table/y=2013/m=01/d=01' }
    end

    context 'when some stale targets (second source)' do
      before do
        fs.touch!('log/20130101.random_2.log', mtime: future_time)
      end

      it { expect(stale_targets.size).to eq(1) }
      it { expect(stale_targets).to include 'table/y=2013/m=01/d=01' }
    end

    context 'when some stale targets (tie breaker)' do
      before do
        fs.touch!('log/20130101.random_1.log', mtime: present_time)
      end

      it { expect(stale_targets.size).to eq(1) }
      it { expect(stale_targets).to include 'table/y=2013/m=01/d=01' }
    end

    context 'when all stale targets' do
      before do
        fs.touch!('log/20130101.random_1.log', mtime: future_time)
        fs.touch!('log/20130102.random_2.log', mtime: future_time)
        fs.touch!('log/20130103.random_1.log', mtime: future_time)
        fs.touch!('log/20130103.random_2.log', mtime: future_time)
      end

      it { expect(stale_targets.size).to eq(3) }
      it { expect(stale_targets).to include 'table/y=2013/m=01/d=01' }
      it { expect(stale_targets).to include 'table/y=2013/m=01/d=02' }
      it { expect(stale_targets).to include 'table/y=2013/m=01/d=03' }
    end

    context 'when missing source last_modified_at' do
      before do
        fs.touch!('log/20130101.random_1.log', mtime: Masamune::DataPlanElem::MISSING_MODIFIED_AT)
      end

      it { expect(stale_targets).to be_empty }
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
      it 'has 6 items' do
        expect(subject.size).to eq(6)
      end
      it { is_expected.to include 'table/y=2012/m=12/d=29' }
      it { is_expected.to include 'table/y=2012/m=12/d=30' }
      it { is_expected.to include 'table/y=2012/m=12/d=31' }
      it { is_expected.to include 'table/y=2013/m=01/d=01' }
      it { is_expected.to include 'table/y=2013/m=01/d=02' }
      it { is_expected.to include 'table/y=2013/m=02/d=01' }
    end

    context 'when :month' do
      let(:grain) { :month }
      it 'has 3 items' do
        expect(subject.size).to eq(3)
      end
      it { is_expected.to include 'table/y=2012/m=12' }
      it { is_expected.to include 'table/y=2013/m=01' }
      it { is_expected.to include 'table/y=2013/m=02' }
    end

    context 'when :year' do
      let(:grain) { :year }
      it 'has 2 items' do
        expect(subject.size).to eq(2)
      end
      it { is_expected.to include 'table/y=2012' }
      it { is_expected.to include 'table/y=2013' }
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

      it { is_expected.to eq(true) }
    end

    context 'with basic enum and wildcard elem' do
      let(:enum) { ['log/20130101.random_1.log', 'log/20130102.random_2.log'] }
      let(:elem) { 'log/20130101.*.log' }

      it { is_expected.to eq(false) }
    end

    context 'with wildcard enum and wildcard elem' do
      let(:enum) { ['log/20130101.*.log', 'log/20130102.*.log'] }
      let(:elem) { 'log/20130101.*.log' }

      it { is_expected.to eq(true) }
    end

    context 'with wildcard enum and basic elem' do
      let(:enum) { ['log/20130101.*.log', 'log/20130102.*.log'] }
      let(:elem) { 'log/20130101.random_1.log' }

      it { is_expected.to eq(false)  }
    end
  end
end
