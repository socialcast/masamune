require 'spec_helper'

describe Masamune::Context do
  let(:instance) { described_class.new }

  describe '#with_exclusive_lock' do
    context 'when lock can be acquired' do
      before do
        File.any_instance.should_receive(:flock).twice.and_return(0)
      end
      it { expect { |b| instance.with_exclusive_lock('some_lock', &b) }.to yield_control }
    end

    context 'when lock cannot be acquired' do
      before do
        File.any_instance.should_receive(:flock).twice.and_return(1)
      end

      it { expect { |b| instance.with_exclusive_lock('some_lock', &b) }.to raise_error /acquire lock attempt failed for 'some_lock'/ }
    end
  end
end
