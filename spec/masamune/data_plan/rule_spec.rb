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

describe Masamune::DataPlan::Rule do
  let(:engine) { Masamune::DataPlan::Engine.new }
  let(:name) { 'primary' }
  let(:type) { :target }
  let(:pattern) { 'report/%Y-%m-%d/%H' }
  let(:options) { { path: pattern } }

  let(:instance) { described_class.new(engine, name, type, options) }

  describe '#pattern' do
    subject do
      instance.pattern
    end

    context 'with string' do
      let(:pattern) { 'report/%Y-%m-%d/%H' }
      it { is_expected.to eq('report/%Y-%m-%d/%H') }
    end

    context 'with lambda' do
      let(:pattern) { ->(_) { 'report/%Y-%m-%d/%H' } }
      it { is_expected.to eq('report/%Y-%m-%d/%H') }
    end
  end

  describe '#bind_date_or_time' do
    subject(:elem) { instance.bind_date_or_time(input) }

    context 'with nil input' do
      let(:input) { nil }
      it { expect { elem }.to raise_error ArgumentError }
    end

    context 'with unknown input type' do
      let(:input) { 1 }
      it { expect { elem }.to raise_error ArgumentError }
    end

    context 'with DateTime input' do
      let(:input) { DateTime.civil(2013, 0o4, 0o5, 23, 13) }

      describe '#path' do
        subject { elem.path }
        it { is_expected.to eq('report/2013-04-05/23') }
      end
    end

    context 'with DateTime input and unix timestamp pattern' do
      let(:pattern) { 'logs/%H-s.log' }
      let(:input) { DateTime.civil(2013, 0o4, 0o5, 23, 13) }

      describe '#path' do
        subject { elem.path }
        it { is_expected.to eq('logs/1365202800.log') }
      end
    end

    context 'with Date input' do
      let(:input) { Date.civil(2013, 0o4, 0o5) }

      describe '#path' do
        subject { elem.path }
        it { is_expected.to eq('report/2013-04-05/00') }
      end
    end
  end

  describe '#bind_input' do
    subject(:elem) { instance.bind_input(input) }

    context 'with default' do
      let(:input) { 'report/2013-04-05/23' }
      let(:output_date) { DateTime.civil(2013, 0o4, 0o5, 23) }

      describe '#path' do
        subject { elem.path }
        it { is_expected.to eq(input) }
      end

      describe '#start_time' do
        subject { elem.start_time }
        it { is_expected.to eq(output_date) }
      end

      describe '#stop_time' do
        subject { elem.stop_time }
        it { is_expected.to eq(output_date.to_time + 1.hour) }
      end
    end

    context 'with unix timestamp pattern' do
      let(:pattern) { 'logs/%H-s.log' }
      let(:input) { 'logs/1365202800.log' }
      let(:output_date) { DateTime.civil(2013, 0o4, 0o5, 23) }

      describe '#path' do
        subject { elem.path }
        it { is_expected.to eq(input) }
      end

      describe '#start_time' do
        subject { elem.start_time }
        it { is_expected.to eq(output_date) }
      end

      describe '#stop_time' do
        subject { elem.stop_time }
        it { is_expected.to eq(output_date.to_time + 1.hour) }
      end
    end

    context 'with previously bound input' do
      let(:prev_input) { 'report/2013-04-05/23' }
      let(:input) { instance.bind_input(prev_input) }
      it { is_expected.to eq(input) }
    end

    context 'with wildcard pattern' do
      let(:pattern) { 'requests/y=%Y/m=%-m/d=%-d/h=%-k/*' }
      let(:input) { 'requests/y=2013/m=4/d=30/h=20/part-00000' }
      let(:output_date) { DateTime.civil(2013, 0o4, 30, 20) }

      describe '#path' do
        subject { elem.path }
        it { is_expected.to eq(input) }
      end

      describe '#start_time' do
        subject { elem.start_time }
        it { is_expected.to eq(output_date) }
      end

      describe '#stop_time' do
        subject { elem.stop_time }
        it { is_expected.to eq(output_date.to_time + 1.hour) }
      end
    end
  end

  describe '#unify' do
    let(:primary) { described_class.new(engine, name, type, path: 'report/%Y-%m-%d/%H') }
    let(:induced) { described_class.new(engine, name, type, path: 'table/y=%Y/m=%m/d=%d/h=%H') }
    let(:elem) { primary.bind_input(input) }

    subject(:new_elem) { instance.unify(elem, induced) }

    context 'when input fully matches basis pattern' do
      let(:input) { 'report/2013-01-02/00' }

      describe '#path' do
        subject { new_elem.path }
        it { is_expected.to eq('table/y=2013/m=01/d=02/h=00') }
      end
    end

    context 'when input partially matches basis pattern' do
      let(:induced) { described_class.new(engine, name, type, path: 'table/%Y-%m') }

      let(:input) { 'report/2013-01-02/00' }

      describe '#path' do
        subject { new_elem.path }
        it { is_expected.to eq('table/2013-01') }
      end
    end
  end

  describe '#matches' do
    subject do
      instance.matches?(input)
    end

    context 'when input fully matches' do
      let(:input) { 'report/2013-01-02/00' }
      it { is_expected.to eq(true) }
    end

    context 'when input under matches' do
      let(:input) { 'report/2013-01-02' }
      it { is_expected.to eq(false) }
    end

    context 'when input over matches' do
      let(:pattern) { 'report/%Y-%m-%d' }
      let(:input) { 'report/2013-01-02/00' }
      it { is_expected.to eq(false) }
    end

    context 'when input does not match' do
      let(:input) { 'report' }
      it { is_expected.to eq(false) }
    end

    context 'with alternative hour' do
      let(:pattern) { 'requests/y=%Y/m=%-m/d=%-d/h=%-k' }
      let(:input) { 'requests/y=2013/m=5/d=1/h=1' }
      it { is_expected.to eq(true) }
    end

    context 'with another alternative hour' do
      let(:pattern) { 'requests/y=%Y/m=%-m/d=%-d/h=%-k' }
      let(:input) { 'requests/y=2013/m=4/d=30/h=20' }
      it { is_expected.to eq(true) }
    end

    context 'with wildcard pattern' do
      let(:pattern) { 'request_logs/%Y%m%d*request.log' }
      let(:input) { 'request_logs/20130524.random.request.log' }
      it { is_expected.to eq(true) }
    end

    context 'with wildcard input' do
      let(:pattern) { 'requests/y=%Y/m=%-m/d=%-d/h=%-k' }
      let(:input) { 'requests/y=2013/m=4/d=30/h=20/*' }
      it { is_expected.to eq(true) }
    end
  end

  describe '#generate' do
    context 'with a block' do
      let(:start_date) { DateTime.civil(2013, 0o4, 0o5, 20) }
      let(:stop_date) { DateTime.civil(2013, 0o4, 0o5, 20) }
      specify { expect { |b| instance.generate(start_date, stop_date, &b) }.to yield_control }
    end

    context 'without a block' do
      let(:start_date) { DateTime.civil(2013, 0o4, 0o5, 20) }
      let(:stop_date) { DateTime.civil(2013, 0o4, 0o5, 22) }

      subject(:elems) do
        instance.generate(start_date, stop_date)
      end

      it { expect(elems.map(&:path)).to eq(['report/2013-04-05/20', 'report/2013-04-05/21', 'report/2013-04-05/22']) }
    end
  end

  describe '#time_step' do
    subject { instance.time_step }

    context '24 hour' do
      let(:pattern) { '%Y-%m-%d/%k' }
      it { is_expected.to eq(:hours) }
    end
    context '24 hour (condensed)' do
      let(:pattern) { '%Y-%m-%d/%-k' }
      it { is_expected.to eq(:hours) }
    end
    context '12 hour' do
      let(:pattern) { '%Y-%m-%d/%H' }
      it { is_expected.to eq(:hours) }
    end
    context '12 hour (condensed)' do
      let(:pattern) { '%Y-%m-%d/%-H' }
      it { is_expected.to eq(:hours) }
    end
    context 'daily' do
      let(:pattern) { '%Y-%m-%d' }
      it { is_expected.to eq(:days) }
    end
    context 'monthly' do
      let(:pattern) { '%Y-%m' }
      it { is_expected.to eq(:months) }
    end
    context 'yearly' do
      let(:pattern) { '%Y' }
      it { is_expected.to eq(:years) }
    end
    context 'hourly unix' do
      let(:pattern) { '%H-s' }
      it { is_expected.to eq(:hours) }
    end
    context 'daily unix' do
      let(:pattern) { '%d-s' }
      it { is_expected.to eq(:days) }
    end
    context 'monthly unix' do
      let(:pattern) { '%m-s' }
      it { is_expected.to eq(:months) }
    end
    context 'yearly unix' do
      let(:pattern) { '%Y-s' }
      it { is_expected.to eq(:years) }
    end
  end

  describe '#time_round' do
    let(:input_time) { DateTime.civil(2013, 9, 13, 23, 13) }
    subject { instance.time_round(input_time) }

    before do
      allow(instance).to receive(:time_step) { time_step }
    end

    context 'hourly' do
      let(:time_step) { :hours }
      it { is_expected.to eq(DateTime.civil(2013, 9, 13, 23)) }
    end
    context 'daily' do
      let(:time_step) { :days }
      it { is_expected.to eq(DateTime.civil(2013, 9, 13)) }
    end
    context 'monthly' do
      let(:time_step) { :months }
      it { is_expected.to eq(DateTime.civil(2013, 9)) }
    end
    context 'yearly' do
      let(:time_step) { :years }
      it { is_expected.to eq(DateTime.civil(2013)) }
    end
  end

  describe '#round' do
    subject(:new_instance) { instance.round(grain) }

    context 'with totally partitioned pattern' do
      let(:pattern) { 'table/y=%Y/m=%m/d=%d/h=%H' }
      context 'with :hour' do
        let(:grain) { :hour }

        describe '#pattern' do
          subject { new_instance.pattern }
          it { is_expected.to eq('table/y=%Y/m=%m/d=%d/h=%H') }
        end
      end

      context 'with :day' do
        let(:grain) { :day }

        describe '#pattern' do
          subject { new_instance.pattern }
          it { is_expected.to eq('table/y=%Y/m=%m/d=%d') }
        end
      end

      context 'with :month' do
        let(:grain) { :month }

        describe '#pattern' do
          subject { new_instance.pattern }
          it { is_expected.to eq('table/y=%Y/m=%m') }
        end
      end

      context 'with :year' do
        let(:grain) { :year }

        describe '#pattern' do
          subject { new_instance.pattern }
          it { is_expected.to eq('table/y=%Y') }
        end
      end
    end

    context 'with partially partitioned pattern' do
      let(:pattern) { 'table/%Y-%m-%d/%H' }

      context 'with :hour' do
        let(:grain) { :hour }

        describe '#pattern' do
          subject { new_instance.pattern }
          it { is_expected.to eq('table/%Y-%m-%d/%H') }
        end
      end

      context 'with :day' do
        let(:grain) { :day }

        describe '#pattern' do
          subject { new_instance.pattern }
          it { is_expected.to eq('table/%Y-%m-%d') }
        end
      end

      context 'with :month' do
        let(:grain) { :month }

        describe '#pattern' do
          subject { new_instance.pattern }
          it { is_expected.to eq('table/%Y-%m-%d') }
        end
      end

      context 'with :year' do
        let(:grain) { :year }

        describe '#pattern' do
          subject { new_instance.pattern }
          it { is_expected.to eq('table/%Y-%m-%d') }
        end
      end
    end

    context 'when cannot round due to granularity' do
      shared_context 'cannot round' do
        it { expect { subject }.to raise_error RuntimeError, /cannot round to :#{grain} for #{pattern}/ }
      end

      context 'with :hour' do
        let(:grain) { :hour }
        let(:pattern) { 'table/y=%Y/m=%m/d=%d' }
        include_context 'cannot round'
      end

      context 'with :day' do
        let(:grain) { :day }
        let(:pattern) { 'table/y=%Y/m=%m' }
        include_context 'cannot round'
      end

      context 'with :month' do
        let(:grain) { :month }
        let(:pattern) { 'table/y=%Y' }
        include_context 'cannot round'
      end
    end
  end

  describe '#free? ' do
    subject { instance.free? }
    context 'with rule that contains free variables' do
      let(:pattern) { 'report/%Y-%m-%d/%H' }
      it { is_expected.to be(true) }
    end

    context 'with rule that does not contain free variables' do
      let(:pattern) { 'report/file' }
      it { is_expected.to be(false) }
    end
  end

  describe '#bound? ' do
    subject { instance.bound? }
    context 'with rule that contains free variables' do
      let(:pattern) { 'report/%Y-%m-%d/%H' }
      it { is_expected.to be(false) }
    end

    context 'with rule that does not contain free variables' do
      let(:pattern) { 'report/file' }
      it { is_expected.to be(true) }
    end
  end
end
