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

describe Masamune::DataPlan::Engine do
  let(:filesystem) { Masamune::MockFilesystem.new }
  let(:environment) { Masamune::Environment.new }
  let(:engine) { Masamune::DataPlan::Engine.new }

  before do
    environment.filesystem = filesystem
    engine.environment = environment
  end

  let(:command) do
    proc do |engine, rule|
      missing_targets = []
      engine.targets(rule).missing.each do |target|
        missing_targets << target.path if target.sources.existing.any?
      end
      engine.filesystem.touch!(*missing_targets.map { |target| File.join(target, 'DATA') }) if missing_targets.any?
    end
  end

  before do
    engine.add_target_rule('non_primary', path: '/table/y=%Y/m=%m/d=%d', primary: false)
    engine.add_source_rule('non_primary', path: '/log/%Y%m%d.*.log', primary: false)
    engine.add_command_rule('non_primary', ->(*_) { raise })
    engine.add_target_rule('primary', path: '/table/y=%Y/m=%m/d=%d')
    engine.add_source_rule('primary', path: '/log/%Y%m%d.*.log')
    engine.add_command_rule('primary', command)
    engine.add_target_rule('derived_daily', path: '/daily/%Y-%m-%d')
    engine.add_source_rule('derived_daily', path: '/table/y=%Y/m=%m/d=%d')
    engine.add_command_rule('derived_daily', command)
    engine.add_target_rule('derived_monthly', path: '/monthly/%Y-%m')
    engine.add_source_rule('derived_monthly', path: '/table/y=%Y/m=%m/d=%d')
    engine.add_command_rule('derived_monthly', command)
  end

  describe '#filesystem' do
    it { expect(engine.filesystem).to be_a(Masamune::CachedFilesystem) }
    it { expect(environment.filesystem).to be_a(Masamune::MockFilesystem) }
  end

  describe '#targets_for_date_range' do
    let(:start) { Date.civil(2013, 0o1, 0o1) }
    let(:stop) { Date.civil(2013, 0o1, 0o3) }

    subject { engine.targets_for_date_range(rule, start, stop).map(&:path) }

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
      engine.targets_for_source(rule, source)
    end

    context 'primary' do
      let(:rule) { 'primary' }
      let(:source) { '/log/20130101.random.log' }

      it { expect(targets.first.start_time).to eq(Date.civil(2013, 0o1, 0o1)) }
      it { expect(targets.first.stop_time).to eq(Date.civil(2013, 0o1, 0o2)) }
      it { expect(targets.first.path).to eq('/table/y=2013/m=01/d=01') }
    end

    context 'derived_daily' do
      let(:rule) { 'derived_daily' }
      let(:source) { '/table/y=2013/m=01/d=01' }

      it { expect(targets.first.start_time).to eq(Date.civil(2013, 0o1, 0o1)) }
      it { expect(targets.first.stop_time).to eq(Date.civil(2013, 0o1, 0o2)) }
      it { expect(targets.first.path).to eq('/daily/2013-01-01') }
    end

    context 'derived_monthly' do
      let(:rule) { 'derived_monthly' }
      let(:source) { '/table/y=2013/m=01/d=01' }

      it { expect(targets.first.start_time).to eq(Date.civil(2013, 0o1, 0o1)) }
      it { expect(targets.first.stop_time).to eq(Date.civil(2013, 0o2, 0o1)) }
      it { expect(targets.first.path).to eq('/monthly/2013-01') }
    end
  end

  describe '#sources_for_target' do
    subject(:sources) do
      engine.sources_for_target(rule, target)
    end

    subject(:existing) do
      sources.existing
    end

    before do
      engine.filesystem.touch!('/log/20130101.app1.log')
      engine.filesystem.touch!('/log/20130101.app2.log')
      engine.filesystem.touch!('/log/20130104.app1.log')
      engine.filesystem.touch!('/log/20130104.app2.log')
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
        it { expect(sources).to include format('/table/y=2013/m=01/d=%02d', day) }
      end
      it { expect(sources.size).to eq(31) }
    end

    context 'invalid target' do
      let(:rule) { 'derived_daily' }
      let(:target) { '/table/y=2013/m=01/d=01' }
      it { expect { subject }.to raise_error(/Cannot bind_input/) }
    end
  end

  describe '#rule_for_target' do
    subject { engine.rule_for_target(target) }

    context 'primary source' do
      let(:target) { '/log/20130101.random_1.log' }
      it { is_expected.to eq(Masamune::DataPlan::Rule::TERMINAL) }
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
      it { expect { subject }.to raise_error(/No rule matches/) }
    end
  end

  describe '#prepare' do
    before do
      engine.prepare(rule, options)
    end

    subject(:targets) do
      engine.targets(rule)
    end

    subject(:sources) do
      engine.sources(rule)
    end

    context 'with :targets' do
      let(:rule) { 'primary' }

      let(:options) { { targets: ['/table/y=2013/m=01/d=01', '/table/y=2013/m=01/d=02', '/table/y=2013/m=01/d=02'] } }

      it { expect(targets).to include '/table/y=2013/m=01/d=01' }
      it { expect(targets).to include '/table/y=2013/m=01/d=02' }
      it { expect(sources).to include '/log/20130101.*.log' }
      it { expect(sources).to include '/log/20130102.*.log' }
    end

    context 'with :sources' do
      let(:rule) { 'derived_daily' }

      let(:options) { { sources: ['/table/y=2013/m=01/d=01', '/table/y=2013/m=01/d=02', '/table/y=2013/m=01/d=02'] } }

      it { expect(targets).to include '/daily/2013-01-01' }
      it { expect(targets).to include '/daily/2013-01-02' }
      it { expect(sources).to include '/table/y=2013/m=01/d=01' }
      it { expect(sources).to include '/table/y=2013/m=01/d=02' }
    end
  end

  describe '#execute' do
    let(:options) { {} }

    before do
      engine.prepare(rule, targets: targets)
    end

    subject(:execute) do
      engine.execute(rule, options)
    end

    context 'primary rule' do
      let(:rule) { 'primary' }
      let(:targets) do
        [
          '/table/y=2013/m=01/d=01',
          '/table/y=2013/m=01/d=02',
          '/table/y=2013/m=01/d=03'
        ]
      end

      context 'when target data exists' do
        before do
          engine.filesystem.touch!('/table/y=2013/m=01/d=01', '/table/y=2013/m=01/d=02', '/table/y=2013/m=01/d=03')
          expect(engine.filesystem).to receive(:touch!).never
          execute
        end

        it 'should not call touch!' do
        end
      end

      context 'when partial target data exists' do
        before do
          engine.filesystem.touch!('/log/20130101.app1.log', '/log/20130102.app1.log', '/log/20130103.app1.log')
          engine.filesystem.touch!('/table/y=2013/m=01/d=01/DATA', '/table/y=2013/m=01/d=03/DATA')
          expect(engine.filesystem).to receive(:touch!).with('/table/y=2013/m=01/d=02/DATA').and_call_original
          execute
        end

        it 'should call touch!' do
        end
      end

      context 'when source data does not exist' do
        before do
          expect(engine.filesystem).to receive(:touch!).never
          execute
        end

        it 'should not call touch!' do
        end
      end
    end

    shared_examples_for 'derived daily data' do
      context 'when primary target data exists' do
        let(:derived_targets) { ['/table/y=2013/m=01/d=01/DATA', '/table/y=2013/m=01/d=02/DATA', '/table/y=2013/m=01/d=03/DATA'] }

        before do
          engine.filesystem.touch!('/log/20130101.app1.log', '/log/20130102.app1.log', '/log/20130103.app1.log')
          expect(engine.filesystem).to receive(:touch!).with(*derived_targets).and_call_original
          expect(engine.filesystem).to receive(:touch!).with(*targets).and_call_original
          execute
        end

        it 'should call touch!' do
        end
      end

      context 'when primary target data exists and :resolve is false' do
        let(:options) { { resolve: false } }

        before do
          engine.filesystem.touch!('/log/20130101.app1.log', '/log/20130102.app1.log', '/log/20130103.app1.log')
          expect(engine.filesystem).not_to receive(:touch!)
          execute
        end

        it 'should not call touch!' do
        end
      end
    end

    context 'derived_daily rule' do
      let(:rule) { 'derived_daily' }
      let(:targets) { ['/daily/2013-01-01/DATA', '/daily/2013-01-02/DATA', '/daily/2013-01-03/DATA'] }

      it_behaves_like 'derived daily data' do
        let(:derived_command) { derived_daily_command }
      end
    end

    context 'derived_monthly rule' do
      let(:rule) { 'derived_monthly' }
      let(:targets) { ['/monthly/2013-01/DATA'] }

      it_behaves_like 'derived daily data' do
        let(:derived_command) { derived_monthly_command }
      end
    end
  end

  context 'recursive engines' do
    before do
      engine.add_target_rule('primary', path: '/table/y=%Y/m=%m/d=%d')
      engine.add_source_rule('primary', path: '/log/%Y%m%d.*.log')
      engine.add_source_rule('derived', path: '/table/y=%Y/m=%m/d=%d')
      engine.add_target_rule('derived', path: '/log/%Y%m%d.*.log')
    end

    it 'should raise exception' do
      expect { engine.prepare('derived', targets: ['/log/20140228.wtf.log']) }.to raise_error(/Max depth .* exceeded for rule 'derived'/)
    end
  end
end
