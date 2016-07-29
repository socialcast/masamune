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

describe Masamune::Environment do
  let(:instance) { described_class.new }
  let(:run_dir) { Dir.mktmpdir('masamune') }
  let(:log_dir) { Dir.mktmpdir('masamune') }

  describe '#log_file_name' do
    subject { instance.log_file_name }

    context 'when log_dir defined' do
      before do
        instance.filesystem.add_path(:log_dir, log_dir)
      end

      it { is_expected.to eq(File.join(log_dir, instance.log_file_template)) }
    end

    context 'when log_dir not defined' do
      it { is_expected.to be_nil }
    end
  end

  describe '#with_exclusive_lock' do
    context 'when run_dir not defined' do
      it { expect { |b| instance.with_exclusive_lock('some_lock', &b) }.to raise_error(/filesystem path :run_dir not defined/) }
    end

    context 'when lock can be acquired' do
      before do
        instance.filesystem.add_path(:run_dir, run_dir)
        expect_any_instance_of(File).to receive(:flock).twice.and_return(0)
        expect(instance.logger).to receive(:debug).with("acquiring lock 'some_lock'")
        expect(instance.logger).to receive(:debug).with("releasing lock 'some_lock'")
      end
      it { expect { |b| instance.with_exclusive_lock('some_lock', &b) }.to yield_control }
    end

    context 'with lock configuration' do
      before do
        instance.filesystem.add_path(:run_dir, run_dir)
        instance.configuration.lock = 'long_running'
        expect_any_instance_of(File).to receive(:flock).twice.and_return(0)
        expect(instance.logger).to receive(:debug).with("acquiring lock 'some_lock:long_running'")
        expect(instance.logger).to receive(:debug).with("releasing lock 'some_lock:long_running'")
      end
      it { expect { |b| instance.with_exclusive_lock('some_lock', &b) }.to yield_control }
    end

    context 'when lock cannot be acquired and returns 1' do
      before do
        instance.filesystem.add_path(:run_dir, run_dir)
        expect(instance.logger).to receive(:error).with(/acquire lock attempt failed for 'some_lock'/)
        expect_any_instance_of(File).to receive(:flock).once.and_return(1)
      end

      it { expect { |b| instance.with_exclusive_lock('some_lock', &b) }.to_not raise_error }
    end

    context 'when lock cannot be acquired and returns false' do
      before do
        instance.filesystem.add_path(:run_dir, run_dir)
        expect(instance.logger).to receive(:error).with(/acquire lock attempt failed for 'some_lock'/)
        expect_any_instance_of(File).to receive(:flock).once.and_return(false)
      end

      it { expect { |b| instance.with_exclusive_lock('some_lock', &b) }.to_not raise_error }
    end
  end
end
