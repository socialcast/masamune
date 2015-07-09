#  The MIT License (MIT)
#
#  Copyright (c) 2014-2015, VMware, Inc. All Rights Reserved.
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

require 'spec_helper'

describe Masamune::Environment do
  let(:instance) { described_class.new }
  let(:run_dir) { Dir.mktmpdir('masamune') }

  describe '#with_exclusive_lock' do
    context 'when run_dir not defined' do
      it { expect { |b| instance.with_exclusive_lock('some_lock', &b) }.to raise_error /filesystem path :run_dir not defined/ }
    end

    context 'when lock can be acquired' do
      before do
        instance.filesystem.add_path(:run_dir, run_dir)
        expect_any_instance_of(File).to receive(:flock).twice.and_return(0)
        expect(instance.logger).to receive(:debug).with(%q{acquiring lock 'some_lock'})
        expect(instance.logger).to receive(:debug).with(%q{releasing lock 'some_lock'})
      end
      it { expect { |b| instance.with_exclusive_lock('some_lock', &b) }.to yield_control }
    end

    context 'with lock configuration' do
      before do
        instance.filesystem.add_path(:run_dir, run_dir)
        instance.configuration.lock = 'long_running'
        expect_any_instance_of(File).to receive(:flock).twice.and_return(0)
        expect(instance.logger).to receive(:debug).with(%q{acquiring lock 'some_lock:long_running'})
        expect(instance.logger).to receive(:debug).with(%q{releasing lock 'some_lock:long_running'})
      end
      it { expect { |b| instance.with_exclusive_lock('some_lock', &b) }.to yield_control }
    end

    context 'when lock cannot be acquired' do
      before do
        instance.filesystem.add_path(:run_dir, run_dir)
        expect(instance.logger).to receive(:error).with(/acquire lock attempt failed for 'some_lock'/)
        expect_any_instance_of(File).to receive(:flock).once.and_return(1)
      end

      it { expect { |b| instance.with_exclusive_lock('some_lock', &b) }.to_not raise_error }
    end
  end
end
