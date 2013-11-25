require 'spec_helper'

describe Masamune::Commands::Shell do
  let(:input) { nil }
  let(:options) { {fail_fast: false} }
  let(:delegate) { Masamune::MockDelegate.new(command, input) }
  let(:instance) { described_class.new(delegate, options) }

  describe '#execute' do
    subject do
      instance.execute
    end

    context 'with simple command that succeeds' do
      let(:command) { %Q{echo 'stdout 1'; echo 'stderr 1' 1>&2; echo 'stdout 2'; echo 'stderr 2' 1>&2} }

      before do
        subject
      end

      it { delegate.status.should == 0 }
      it { delegate.stdout.should == ['stdout 1', 'stdout 2'] }
      it { delegate.stderr.should == ['stderr 1', 'stderr 2'] }
    end

    context 'with simple command that fails' do
      let(:command) { %Q{exit 1;} }

      before do
        subject
      end

      it { delegate.status.should == 1 }
      it { delegate.stdout.should == [] }
      it { delegate.stderr.should == [] }
    end

    context 'with fail_fast and simple command that fails' do
      let(:command) { %Q{exit 1;} }
      let(:options) { {fail_fast: true} }
      it { expect { subject }.to raise_error RuntimeError, 'fail_fast' }
    end

    context 'when command is interrupted' do
      let(:command) { %Q{echo 'test'} }

      before do
        delegate.should_receive(:after_execute) { raise Interrupt }
        subject
      end

      it { delegate.status.should == 130 }
    end

    context 'with simple command with input' do
      let(:command) { 'cat' }
      let(:input) { "ping\npong" }

      before do
        subject
      end

      it { delegate.status.should == 0 }
      it { delegate.stdout.should == ['ping', 'pong'] }
      it { delegate.stderr.should == [] }
    end

    context 'with simple command with not-ready input' do
      let(:command) { 'cat' }
      let(:input) { "ping\npong" }

      before do
        IO.any_instance.should_receive(:wait_writable).and_return(nil)
      end

      it { expect { subject }.to raise_error RuntimeError, /IO stdin not ready/ }
    end
  end
end
