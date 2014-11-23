require 'spec_helper'

describe Masamune::DataPlan do
  let(:filesystem) { Masamune::MockFilesystem.new }
  let(:environment) { Masamune::Environment.new }
  let(:plan) { Masamune::DataPlan.new }

  before do
    environment.filesystem = filesystem
    plan.environment = environment
  end

  let(:command) do
    Proc.new do |plan, rule|
      missing_targets = []
      plan.targets(rule).missing.each do |target|
        missing_targets << target.path if target.sources.existing.any?
      end
      plan.filesystem.touch!(*missing_targets) if missing_targets.any?
    end
  end

  before do
    plan.add_target_rule('non_primary', path: '/table/y=%Y/m=%m/d=%d', primary: false)
    plan.add_source_rule('non_primary', path: '/log/%Y%m%d.*.log', primary: false)
    plan.add_command_rule('non_primary', ->(*_) { fail } )
    plan.add_target_rule('primary', path: '/table/y=%Y/m=%m/d=%d')
    plan.add_source_rule('primary', path: '/log/%Y%m%d.*.log')
    plan.add_command_rule('primary', command)
    plan.add_target_rule('derived_daily', path: '/daily/%Y-%m-%d')
    plan.add_source_rule('derived_daily', path: '/table/y=%Y/m=%m/d=%d')
    plan.add_command_rule('derived_daily', command)
    plan.add_target_rule('derived_monthly', path: '/monthly/%Y-%m')
    plan.add_source_rule('derived_monthly', path: '/table/y=%Y/m=%m/d=%d')
    plan.add_command_rule('derived_monthly', command)
  end

  after do
    plan.filesystem.clear!
  end

  describe '#filesystem' do
    it { expect(plan.filesystem).to be_a(Masamune::CachedFilesystem) }
  end

  describe '#targets_for_date_range' do
    let(:start) { Date.civil(2013,01,01) }
    let(:stop) { Date.civil(2013,01,03) }

    subject { plan.targets_for_date_range(rule, start, stop).map(&:path) }

    context 'primary' do
      let(:rule) { 'primary' }
      it { is_expected.to include '/table/y=2013/m=01/d=01' }
      it { is_expected.to include '/table/y=2013/m=01/d=02' }
      it { is_expected.to include '/table/y=2013/m=01/d=03' }
      it 'has 3 items' do
        expect(subject.size).to eq(3)
      end
    end

    context 'derived_daily' do
      let(:rule) { 'derived_daily' }
      it { is_expected.to include '/daily/2013-01-01' }
      it { is_expected.to include '/daily/2013-01-02' }
      it { is_expected.to include '/daily/2013-01-03' }
      it 'has 3 items' do
        expect(subject.size).to eq(3)
      end
    end

    context 'derived_monthly' do
      let(:rule) { 'derived_monthly' }
      it { is_expected.to include '/monthly/2013-01' }
      it 'has 1 item' do
        expect(subject.size).to eq(1)
      end
    end
  end

  describe '#targets_for_source' do
    subject(:targets) do
      plan.targets_for_source(rule, source)
    end

    context 'primary' do
      let(:rule) { 'primary' }
      let(:source) { '/log/20130101.random.log' }

      it { expect(targets.first.start_time).to eq(Date.civil(2013,01,01)) }
      it { expect(targets.first.stop_time).to eq(Date.civil(2013,01,02)) }
      it { expect(targets.first.path).to eq('/table/y=2013/m=01/d=01') }
    end

    context 'derived_daily' do
      let(:rule) { 'derived_daily' }
      let(:source)  { '/table/y=2013/m=01/d=01' }

      it { expect(targets.first.start_time).to eq(Date.civil(2013,01,01)) }
      it { expect(targets.first.stop_time).to eq(Date.civil(2013,01,02)) }
      it { expect(targets.first.path).to eq('/daily/2013-01-01') }
    end

    context 'derived_monthly' do
      let(:rule) { 'derived_monthly' }
      let(:source)  { '/table/y=2013/m=01/d=01' }

      it { expect(targets.first.start_time).to eq(Date.civil(2013,01,01)) }
      it { expect(targets.first.stop_time).to eq(Date.civil(2013,02,01)) }
      it { expect(targets.first.path).to eq('/monthly/2013-01') }
    end
  end

  describe '#sources_for_target' do
    subject(:sources) do
      plan.sources_for_target(rule, target)
    end

    subject(:existing) do
      sources.existing
    end

    before do
      plan.filesystem.touch!('/log/20130101.app1.log')
      plan.filesystem.touch!('/log/20130101.app2.log')
      plan.filesystem.touch!('/log/20130104.app1.log')
      plan.filesystem.touch!('/log/20130104.app2.log')
    end

    context 'valid target associated with wildcard source' do
      let(:rule) { 'primary' }
      let(:target) { '/table/y=2013/m=01/d=01' }

      it { expect(sources.size).to eq(1) }
      it { expect(sources).to include '/log/20130101.*.log' }
      it { expect(existing.size).to eq(2) }
      it { expect(existing).to include '/log/20130101.app1.log' }
      it { expect(existing).to include '/log/20130101.app2.log' }
    end

    context 'valid target associated with a single source file' do
      let(:rule) { 'derived_daily' }
      let(:target) { '/daily/2013-01-03' }

      it { expect(sources).to include '/table/y=2013/m=01/d=03' }
    end

    context 'valid target associated with a group of source files' do
      let(:rule) { 'derived_monthly' }
      let(:target) { '/monthly/2013-01' }

      (1..31).each do |day|
        it { expect(sources).to include '/table/y=2013/m=01/d=%02d' % day }
      end
      it { expect(sources.size).to eq(31) }
    end

    context 'invalid target' do
      let(:rule) { 'derived_daily' }
      let(:target) {  '/table/y=2013/m=01/d=01' }
      it { expect { subject }.to raise_error }
    end
  end

  describe '#rule_for_target' do
    subject { plan.rule_for_target(target) }

    context 'primary source' do
      let(:target) { '/log/20130101.random_1.log' }
      it { is_expected.to eq(Masamune::DataPlanRule::TERMINAL) }
    end

    context 'primary target' do
      let(:target) { '/table/y=2013/m=01/d=01' }
      it { is_expected.to eq('primary') }
    end

    context 'derived_daily target' do
      let(:target) { '/daily/2013-01-03' }
      it { is_expected.to eq('derived_daily') }
    end

    context 'derived_monthly target' do
      let(:target) { '/monthly/2013-01' }
      it { is_expected.to eq('derived_monthly') }
    end

    context 'invalid target' do
      let(:target) { '/daily' }
      it { expect { subject }.to raise_error }
    end
  end

  describe '#prepare' do
    before do
      plan.prepare(rule, options)
    end

    subject(:targets) do
      plan.targets(rule)
    end

    subject(:sources) do
      plan.sources(rule)
    end

    context 'with :targets' do
      let(:rule) { 'primary' }

      let(:options) { {targets: ['/table/y=2013/m=01/d=01', '/table/y=2013/m=01/d=02', '/table/y=2013/m=01/d=02']} }

      it { expect(targets).to include '/table/y=2013/m=01/d=01' }
      it { expect(targets).to include '/table/y=2013/m=01/d=02' }
      it { expect(sources).to include '/log/20130101.*.log' }
      it { expect(sources).to include '/log/20130102.*.log' }
    end

    context 'with :sources' do
      let(:rule) { 'derived_daily' }

      let(:options) { {sources: ['/table/y=2013/m=01/d=01', '/table/y=2013/m=01/d=02', '/table/y=2013/m=01/d=02']} }

      it { expect(targets).to include "/daily/2013-01-01" }
      it { expect(targets).to include "/daily/2013-01-02" }
      it { expect(sources).to include '/table/y=2013/m=01/d=01' }
      it { expect(sources).to include '/table/y=2013/m=01/d=02' }
    end
  end

  describe '#targets' do
    let(:rule) { 'primary' }

    let(:targets) { ['/table/y=2013/m=01/d=01', '/table/y=2013/m=01/d=02'] }
    let(:options) { {targets: targets} }

    before do
      plan.prepare(rule, options)
    end

    subject(:missing) do
      plan.targets(rule).missing
    end

    subject(:existing) do
      plan.targets(rule).existing
    end

    context 'when targets are missing' do
      it { expect(missing.size).to eq(2) }
      it { expect(missing).to include '/table/y=2013/m=01/d=01' }
      it { expect(missing).to include '/table/y=2013/m=01/d=02' }
      it { expect(existing).to be_empty }
    end

    context 'when targets exist' do
      before do
        plan.filesystem.touch!('/table/y=2013/m=01/d=01', '/table/y=2013/m=01/d=02')
      end

      it { expect(missing).to be_empty }
      it { expect(existing.size).to eq(2) }
      it { expect(existing).to include '/table/y=2013/m=01/d=01' }
      it { expect(existing).to include '/table/y=2013/m=01/d=02' }
    end
  end

  describe '#sources' do
    let(:rule) { 'primary' }

    let(:sources) { ['/log/20130101.*.log', '/log/20130102.*.log'] }
    let(:options) { {sources: sources} }

    before do
      plan.prepare(rule, options)
    end

    subject(:missing) do
      plan.sources(rule).missing
    end

    subject(:existing) do
      plan.sources(rule).existing
    end

    context 'when sources are missing' do
      it { expect(missing.size).to eq(2) }
      it { expect(missing).to include '/log/20130101.*.log' }
      it { expect(missing).to include '/log/20130102.*.log' }
      it { expect(existing).to be_empty }
    end

    context 'when sources exist' do
      before do
        plan.filesystem.touch!('/log/20130101.app1.log', '/log/20130101.app2.log', '/log/20130102.app1.log', '/log/20130102.app2.log')
      end

      it { expect(missing).to be_empty }
      it { expect(existing).to include '/log/20130101.app1.log' }
      it { expect(existing).to include '/log/20130101.app2.log' }
      it { expect(existing).to include '/log/20130102.app1.log' }
      it { expect(existing).to include '/log/20130102.app2.log' }
      it { expect(existing.size).to eq(4) }
    end

    context 'when sources partially exist' do
      before do
        plan.filesystem.touch!('/log/20130101.app1.log', '/log/20130101.app2.log')
      end

      it { expect(missing.size).to eq(1) }
      it { expect(missing).to include '/log/20130102.*.log' }
      it { expect(existing.size).to eq(2) }
      it { expect(existing).to include '/log/20130101.app1.log' }
      it { expect(existing).to include '/log/20130101.app2.log' }
    end
  end

  describe '#execute' do
    let(:options) { {} }

    before do
      plan.prepare(rule, targets: targets)
    end

    subject(:execute) do
      plan.execute(rule, options)
    end

    context 'primary rule' do
      let(:rule) { 'primary' }
      let(:targets) {  [
        '/table/y=2013/m=01/d=01',
        '/table/y=2013/m=01/d=02',
        '/table/y=2013/m=01/d=03' ] }

      context 'when target data exists' do
        before do
          plan.filesystem.touch!('/table/y=2013/m=01/d=01', '/table/y=2013/m=01/d=02', '/table/y=2013/m=01/d=03')
          expect(plan.filesystem).to receive(:touch!).never
          execute
        end

        it 'should not call touch!' do; end
      end

      context 'when partial target data exists' do
        before do
          plan.filesystem.touch!('/log/20130101.app1.log', '/log/20130102.app1.log', '/log/20130103.app1.log')
          plan.filesystem.touch!('/table/y=2013/m=01/d=01', '/table/y=2013/m=01/d=03')
          expect(plan.filesystem).to receive(:touch!).with('/table/y=2013/m=01/d=02').and_call_original
          execute
        end

        it 'should call touch!' do; end
      end

      context 'when source data does not exist' do
        before do
          expect(plan.filesystem).to receive(:touch!).never
          execute
        end

        it 'should not call touch!' do; end
      end
    end

    shared_examples_for 'derived daily data' do
      context 'when primary target data exists' do
        let(:derived_targets) {  ['/table/y=2013/m=01/d=01', '/table/y=2013/m=01/d=02', '/table/y=2013/m=01/d=03'] }

        before do
          plan.filesystem.touch!('/log/20130101.app1.log', '/log/20130102.app1.log', '/log/20130103.app1.log')
          expect(plan.filesystem).to receive(:touch!).with(*derived_targets).and_call_original
          expect(plan.filesystem).to receive(:touch!).with(*targets).and_call_original
          execute
        end

        it 'should call touch!' do; end
      end

      context 'when primary target data exists and :resolve is false' do
        let(:options) { {resolve: false} }

        before do
          plan.filesystem.touch!('/log/20130101.app1.log', '/log/20130102.app1.log', '/log/20130103.app1.log')
          expect(plan.filesystem).not_to receive(:touch!)
          execute
        end

        it 'should not call touch!' do; end
      end
    end

    context 'derived_daily rule' do
      let(:rule) { 'derived_daily' }
      let(:targets) { ['/daily/2013-01-01', '/daily/2013-01-02', '/daily/2013-01-03'] }

      it_behaves_like 'derived daily data' do
        let(:derived_command) { derived_daily_command }
      end
    end

    context 'derived_monthly rule' do
      let(:rule) { 'derived_monthly' }
      let(:targets) {  ['/monthly/2013-01'] }

      it_behaves_like 'derived daily data' do
        let(:derived_command) { derived_monthly_command }
      end
    end
  end

  context 'recursive plans' do
    before do
      plan.add_target_rule('primary', path: '/table/y=%Y/m=%m/d=%d')
      plan.add_source_rule('primary', path: '/log/%Y%m%d.*.log')
      plan.add_source_rule('derived', path: '/table/y=%Y/m=%m/d=%d')
      plan.add_target_rule('derived', path: '/log/%Y%m%d.*.log')
    end

    it 'should raise exception' do
      expect { plan.prepare('derived', targets: ['/log/20140228.wtf.log']) }.to raise_error /Max depth .* exceeded for rule 'derived'/
    end
  end
end
