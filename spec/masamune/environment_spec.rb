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
      end
      it { expect { |b| instance.with_exclusive_lock('some_lock', &b) }.to yield_control }
    end

    context 'when lock cannot be acquired' do
      before do
        instance.filesystem.add_path(:run_dir, run_dir)
        expect(instance.logger).to receive(:error).with(/acquire lock attempt failed for 'some_lock'/)
        expect_any_instance_of(File).to receive(:flock).twice.and_return(1)
      end

      it { expect { |b| instance.with_exclusive_lock('some_lock', &b) }.to_not raise_error }
    end
  end
end
