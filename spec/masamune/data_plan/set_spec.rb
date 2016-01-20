#  The MIT License (MIT)
#
#  Copyright (c) 2014-2016, VMware, Inc. All Rights Reserved.
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in
#  all copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
#  THE SOFTWARE.

describe Masamune::DataPlan::Set do
  let(:fs) { Masamune::MockFilesystem.new }
  let!(:engine) { Masamune::DataPlan::Engine.new }

  let!(:source_rule) { engine.add_source_rule('primary', path: '/log/%Y%m%d.*.log') }
  let!(:target_rule) { engine.add_target_rule('primary', path: '/table/y=%Y/m=%m/d=%d') }

  before do
    engine.filesystem = fs
  end

  describe '#missing' do
    let(:paths) { ['/table/y=2013/m=01/d=01', '/table/y=2013/m=01/d=02', '/table/y=2013/m=01/d=03'] }

    let(:instance) { Masamune::DataPlan::Set.new(target_rule, paths) }

    subject(:missing) do
      instance.missing
    end

    context 'when all missing' do
      it { expect(missing.count).to eq(3) }
      it { expect(missing).to include '/table/y=2013/m=01/d=01' }
      it { expect(missing).to include '/table/y=2013/m=01/d=02' }
      it { expect(missing).to include '/table/y=2013/m=01/d=03' }
    end

    context 'when files missing' do
      before do
        fs.touch!('/table/y=2013/m=01/d=01', '/table/y=2013/m=01/d=02')
      end
      it { expect(missing.count).to eq(3) }
      it { expect(missing).to include '/table/y=2013/m=01/d=01' }
      it { expect(missing).to include '/table/y=2013/m=01/d=02' }
      it { expect(missing).to include '/table/y=2013/m=01/d=03' }
    end

    context 'when some missing' do
      before do
        fs.touch!('/table/y=2013/m=01/d=01/0000', '/table/y=2013/m=01/d=02/0000')
      end
      it { expect(missing.count).to eq(1) }
      it { expect(missing).to include '/table/y=2013/m=01/d=03' }
    end

    context 'when none missing' do
      before do
        fs.touch!('/table/y=2013/m=01/d=01/0000', '/table/y=2013/m=01/d=02/0000', '/table/y=2013/m=01/d=03/0000')
      end
      it { expect(missing).to be_empty }
    end
  end

  describe '#existing' do
    let(:instance) { Masamune::DataPlan::Set.new(source_rule, paths) }

    subject(:existing) do
      instance.existing
    end

    context 'with basic paths' do
      let(:paths) { ['/log/20130101.random_1.log', '/log/20130102.random_1.log'] }

      context 'when none existing' do
        it { expect(existing).to be_empty }
      end

      context 'when some existing' do
        before do
          fs.touch!('/log/20130101.random_1.log', '/log/20130101.random_2.log')
        end
        it { expect(existing.count).to eq(1) }
        it { expect(existing).to include '/log/20130101.random_1.log' }
      end

      context 'when all existing' do
        before do
          fs.touch!('/log/20130101.random_1.log', '/log/20130101.random_2.log')
          fs.touch!('/log/20130102.random_1.log', '/log/20130102.random_2.log')
        end
        it { expect(existing.count).to eq(2) }
        it { expect(existing).to include '/log/20130101.random_1.log' }
        it { expect(existing).to include '/log/20130102.random_1.log' }
      end
    end

    context 'with wildcard paths' do
      let(:paths) { ['/log/20130101.*.log', '/log/20130102.*.log'] }

      context 'when none existing' do
        it { expect(existing).to be_empty }
      end

      context 'when some existing' do
        before do
          fs.touch!('/log/20130101.random_1.log', '/log/20130101.random_2.log')
        end
        it { expect(existing.count).to eq(2) }
        it { expect(existing).to include '/log/20130101.random_1.log' }
        it { expect(existing).to include '/log/20130101.random_2.log' }
      end

      context 'when all existing' do
        before do
          fs.touch!('/log/20130101.random_1.log', '/log/20130101.random_2.log')
          fs.touch!('/log/20130102.random_1.log', '/log/20130102.random_2.log')
        end
        it { expect(existing.count).to eq(4) }
        it { expect(existing).to include '/log/20130101.random_1.log' }
        it { expect(existing).to include '/log/20130101.random_2.log' }
        it { expect(existing).to include '/log/20130102.random_1.log' }
        it { expect(existing).to include '/log/20130102.random_2.log' }
      end
    end
  end

  describe '#adjacent' do
    let(:instance) { Masamune::DataPlan::Set.new(source_rule, paths) }

    subject(:sources) do
      instance.adjacent
    end

    subject(:existing) do
      instance.adjacent.existing
    end

    context 'with window of 1 time_step' do
      let(:paths) { ['/log/20130101.*.log'] }

      before do
        allow(instance.rule).to receive(:window) { 1 }
      end

      it { expect(sources.count).to eq(3) }
      it { expect(sources).to include '/log/20121231.*.log' }
      it { expect(sources).to include '/log/20130101.*.log' }
      it { expect(sources).to include '/log/20130102.*.log' }

      context 'when none existing' do
        it { expect(existing).to be_empty }
      end

      context 'when some existing' do
        before do
          fs.touch!('/log/20130101.random_1.log', '/log/20130101.random_2.log')
        end
        it { expect(existing.count).to eq(2) }
        it { expect(existing).to include '/log/20130101.random_1.log' }
        it { expect(existing).to include '/log/20130101.random_2.log' }
      end

      context 'when all existing' do
        before do
          fs.touch!('/log/20121231.random_1.log', '/log/20121231.random_2.log')
          fs.touch!('/log/20130101.random_1.log', '/log/20130101.random_2.log')
          fs.touch!('/log/20130102.random_1.log', '/log/20130102.random_2.log')
          fs.touch!('/log/20130103.random_1.log', '/log/20130103.random_2.log')
        end
        it { expect(existing.count).to eq(6) }
        it { expect(existing).to include '/log/20121231.random_1.log' }
        it { expect(existing).to include '/log/20121231.random_2.log' }
        it { expect(existing).to include '/log/20130101.random_1.log' }
        it { expect(existing).to include '/log/20130101.random_2.log' }
        it { expect(existing).to include '/log/20130102.random_1.log' }
        it { expect(existing).to include '/log/20130102.random_2.log' }
      end
    end
  end

  describe '#stale' do
    context 'when source rule' do
      let(:paths) { ['/log/20130101.random_1.log', '/log/20130102.random_1.log'] }
      let(:instance) { Masamune::DataPlan::Set.new(source_rule, paths) }

      subject(:stale_sources) do
        instance.stale
      end

      it { expect(stale_sources).to be_empty }
    end

    let(:paths) { ['/table/y=2013/m=01/d=01', '/table/y=2013/m=01/d=02', '/table/y=2013/m=01/d=03'] }
    let(:instance) { Masamune::DataPlan::Set.new(target_rule, paths) }
    let(:past_time) { Time.parse('2013-01-01 09:00:00 +0000') }
    let(:present_time) { Time.parse('2013-01-01 09:30:00 +0000') }
    let(:future_time) { Time.parse('2013-01-01 10:00:00 +0000') }

    before do
      fs.touch!('/log/20130101.random_1.log', '/log/20130101.random_2.log', mtime: past_time)
      fs.touch!('/log/20130102.random_1.log', '/log/20130102.random_2.log', mtime: past_time)
      fs.touch!('/log/20130103.random_1.log', '/log/20130103.random_2.log', mtime: past_time)
      fs.touch!('/table/y=2013/m=01/d=01', '/table/y=2013/m=01/d=02', '/table/y=2013/m=01/d=03', mtime: present_time)
    end

    subject(:stale_targets) do
      instance.stale
    end

    context 'when none stale targets' do
      it { expect(stale_targets).to be_empty }
    end

    context 'when some stale targets (first source)' do
      before do
        fs.touch!('/log/20130101.random_1.log', mtime: future_time)
      end

      it { expect(stale_targets.count).to eq(1) }
      it { expect(stale_targets).to include '/table/y=2013/m=01/d=01' }
    end

    context 'when some stale targets (second source)' do
      before do
        fs.touch!('/log/20130101.random_2.log', mtime: future_time)
      end

      it { expect(stale_targets.count).to eq(1) }
      it { expect(stale_targets).to include '/table/y=2013/m=01/d=01' }
    end

    context 'when none stale targets (tie breaker)' do
      before do
        fs.touch!('/log/20130101.random_1.log', mtime: present_time)
      end

      it { expect(stale_targets).to be_empty }
    end

    context 'when all stale targets' do
      before do
        fs.touch!('/log/20130101.random_1.log', mtime: future_time)
        fs.touch!('/log/20130102.random_2.log', mtime: future_time)
        fs.touch!('/log/20130103.random_1.log', mtime: future_time)
        fs.touch!('/log/20130103.random_2.log', mtime: future_time)
      end

      it { expect(stale_targets.count).to eq(3) }
      it { expect(stale_targets).to include '/table/y=2013/m=01/d=01' }
      it { expect(stale_targets).to include '/table/y=2013/m=01/d=02' }
      it { expect(stale_targets).to include '/table/y=2013/m=01/d=03' }
    end

    context 'when missing source last_modified_at' do
      before do
        fs.touch!('/log/20130101.random_1.log', mtime: Masamune::DataPlan::Elem::MISSING_MODIFIED_AT)
      end

      it { expect(stale_targets).to be_empty }
    end
  end

  describe '#with_grain' do
    let(:paths) { ['/table/y=2012/m=12/d=29', '/table/y=2012/m=12/d=30', '/table/y=2012/m=12/d=31',
                   '/table/y=2013/m=01/d=01', '/table/y=2013/m=01/d=02', '/table/y=2013/m=02/d=01', ] }

    let(:instance) { Masamune::DataPlan::Set.new(target_rule, paths) }

    subject(:granular_targets) do
      instance.with_grain(grain)
    end

    context 'when :day' do
      let(:grain) { :day }
      it 'has 6 items' do
        expect(subject.count).to eq(6)
      end
      it { is_expected.to include '/table/y=2012/m=12/d=29' }
      it { is_expected.to include '/table/y=2012/m=12/d=30' }
      it { is_expected.to include '/table/y=2012/m=12/d=31' }
      it { is_expected.to include '/table/y=2013/m=01/d=01' }
      it { is_expected.to include '/table/y=2013/m=01/d=02' }
      it { is_expected.to include '/table/y=2013/m=02/d=01' }
    end

    context 'when :month' do
      let(:grain) { :month }
      it 'has 3 items' do
        expect(subject.count).to eq(3)
      end
      it { is_expected.to include '/table/y=2012/m=12' }
      it { is_expected.to include '/table/y=2013/m=01' }
      it { is_expected.to include '/table/y=2013/m=02' }
    end

    context 'when :year' do
      let(:grain) { :year }
      it 'has 2 items' do
        expect(subject.count).to eq(2)
      end
      it { is_expected.to include '/table/y=2012' }
      it { is_expected.to include '/table/y=2013' }
    end
  end

  describe '#include?' do
    let(:instance) { Masamune::DataPlan::Set.new(source_rule, enum) }
    subject do
      instance.include?(elem)
    end

    context 'with basic enum and basic elem' do
      let(:enum) { ['/log/20130101.random_1.log', '/log/20130102.random_2.log'] }
      let(:elem) { '/log/20130101.random_1.log' }

      it { is_expected.to eq(true) }
    end

    context 'with basic enum and wildcard elem' do
      let(:enum) { ['/log/20130101.random_1.log', '/log/20130102.random_2.log'] }
      let(:elem) { '/log/20130101.*.log' }

      it { is_expected.to eq(false) }
    end

    context 'with wildcard enum and wildcard elem' do
      let(:enum) { ['/log/20130101.*.log', '/log/20130102.*.log'] }
      let(:elem) { '/log/20130101.*.log' }

      it { is_expected.to eq(true) }
    end

    context 'with wildcard enum and basic elem' do
      let(:enum) { ['/log/20130101.*.log', '/log/20130102.*.log'] }
      let(:elem) { '/log/20130101.random_1.log' }

      it { is_expected.to eq(false)  }
    end
  end

  describe '#incomplete' do
    let!(:source_rule) { engine.add_source_rule('primary', path: '/log/%Y%m%d.*.log') }
    let!(:target_rule) { engine.add_target_rule('primary', path: '/table/y=%Y/m=%m') }

    let(:paths) { ['/log/20140101.random_1.log', '/log/20140102.random_1.log', '/log/20140201.random_1.log', '/log/20140202.random_1.log'] }

    let(:instance) { Masamune::DataPlan::Set.new(source_rule, paths) }

    subject(:incomplete) do
      instance.targets.incomplete
    end

    context 'when all incomplete' do
      it { expect(incomplete.count).to eq(2) }
      it { expect(incomplete).to include '/table/y=2014/m=01' }
      it { expect(incomplete).to include '/table/y=2014/m=02' }
    end

    context 'when some incomplete' do
      before do
        (1..31).each do |day|
          fs.touch!('/log/201401%02d.random_1.log' % day)
        end
      end

      it { expect(incomplete.count).to eq(1) }
      it { expect(incomplete).to include '/table/y=2014/m=02' }
    end

    context 'when none incomplete' do
      before do
        (1..31).each do |day|
          fs.touch!('/log/201401%02d.random_1.log' % day)
          fs.touch!('/log/201402%02d.random_1.log' % day)
        end
      end

      it { expect(incomplete.count).to eq(0) }
    end
  end

  describe '#updatable' do
    let(:paths) { ['/table/y=2013/m=01/d=01', '/table/y=2013/m=01/d=02', '/table/y=2013/m=01/d=03'] }

    let(:instance) { Masamune::DataPlan::Set.new(target_rule, paths) }

    let(:past_time) { Time.parse('2013-01-01 09:00:00 +0000') }
    let(:present_time) { Time.parse('2013-01-01 09:30:00 +0000') }
    let(:future_time) { Time.parse('2013-01-01 10:00:00 +0000') }

    subject(:actionable) do
      instance.actionable
    end

    subject(:updateable) do
      instance.updateable
    end

    context 'when targets are existing' do
      before do
        fs.touch!('/table/y=2013/m=01/d=01/0000', '/table/y=2013/m=01/d=02/0000', '/table/y=2013/m=01/d=03/0000', mtime: present_time)
      end

      context 'when all sources missing' do
        it 'actionable is equivalent to incomplete' do
          expect(actionable).to eq(instance.incomplete)
        end
        it { expect(updateable).to be_empty }
      end

      context 'when all sources existing (stale)' do
        before do
          fs.touch!('/log/20130101.random_1.log', '/log/20130101.random_2.log', mtime: future_time)
          fs.touch!('/log/20130102.random_1.log', '/log/20130102.random_2.log', mtime: future_time)
          fs.touch!('/log/20130103.random_1.log', '/log/20130103.random_2.log', mtime: future_time)
        end
        it 'actionable is equivalent to stale' do
          expect(actionable).to eq(instance.stale)
        end
        it { expect(updateable.count).to eq(3) }
        it { expect(updateable).to include '/table/y=2013/m=01/d=01' }
        it { expect(updateable).to include '/table/y=2013/m=01/d=02' }
        it { expect(updateable).to include '/table/y=2013/m=01/d=03' }
      end

      context 'when all sources existing (fresh)' do
        before do
          fs.touch!('/log/20130101.random_1.log', '/log/20130101.random_2.log', mtime: past_time)
          fs.touch!('/log/20130102.random_1.log', '/log/20130102.random_2.log', mtime: past_time)
          fs.touch!('/log/20130103.random_1.log', '/log/20130103.random_2.log', mtime: past_time)
        end
        it { expect(actionable).to be_empty }
        it { expect(updateable).to be_empty }
      end
    end

    context 'when targets are missing' do
      context 'when all sources missing' do
        it 'actionable is equivalent to incomplete' do
          expect(actionable).to eq(instance.incomplete)
        end
        it { expect(updateable).to be_empty }
      end

      context 'when some sources missing' do
        before do
          fs.touch!('/log/20130101.random_1.log', '/log/20130101.random_2.log')
        end
        it 'actionable is equivalent to missing' do
          expect(actionable).to eq(instance.missing)
        end
        it { expect(updateable.count).to eq(1) }
        it { expect(updateable).to include '/table/y=2013/m=01/d=01' }
      end

      context 'when all sources existing' do
        before do
          fs.touch!('/log/20130101.random_1.log', '/log/20130101.random_2.log')
          fs.touch!('/log/20130102.random_1.log', '/log/20130102.random_2.log')
          fs.touch!('/log/20130103.random_1.log', '/log/20130103.random_2.log')
        end
        it 'actionable is equivalent to missing' do
          expect(actionable).to eq(instance.missing)
        end
        it { expect(updateable.count).to eq(3) }
        it { expect(updateable).to include '/table/y=2013/m=01/d=01' }
        it { expect(updateable).to include '/table/y=2013/m=01/d=02' }
        it { expect(updateable).to include '/table/y=2013/m=01/d=03' }
      end
    end
  end

  context 'when sets are chained together' do
    let!(:source_rule) { engine.add_source_rule('primary', path: '/log/%Y%m%d.*.log') }
    let!(:target_rule) { engine.add_target_rule('primary', path: '/table/y=%Y/m=%m') }

    let(:paths) { ['/log/20140101.random_1.log', '/log/20140102.random_1.log', '/log/20140201.random_1.log', '/log/20140202.random_1.log'] }

    let(:instance) { Masamune::DataPlan::Set.new(source_rule, paths) }

    context 'when sources are missing' do
      it 'should chain expectedly' do
        expect(instance.targets.count).to eq(2)
        expect(instance.targets.sources.existing.count).to eq(0)
        expect(instance.targets.sources.existing.targets.count).to eq(0)
      end
    end

    context 'when sources are present' do
      before do
        fs.touch!(*paths)
      end

      it 'should chain expectedly' do
        expect(instance.targets.count).to eq(2)
        expect(instance.targets.sources.existing.count).to eq(4)
        expect(instance.targets.sources.existing.targets.count).to eq(2)
        expect(instance.targets.sources.existing.targets.sources.existing.count).to eq(4)
      end
    end
  end
end
