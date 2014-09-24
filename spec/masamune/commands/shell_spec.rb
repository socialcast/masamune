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
      let(:command) { %Q{bash -c "echo 'stdout 1'; echo 'stderr 1' 1>&2; echo 'stdout 2'; echo 'stderr 2' 1>&2"} }

      before do
        subject
      end

      it { expect(delegate.status).to eq(0) }
      it { expect(delegate.stdout).to eq(['stdout 1', 'stdout 2']) }
      it { expect(delegate.stderr).to eq(['stderr 1', 'stderr 2']) }
    end

    context 'with simple command that fails' do
      let(:command) { %Q{bash -c 'exit 1'} }

      before do
        subject
      end

      it { expect(delegate.status).to eq(1) }
      it { expect(delegate.stdout).to eq([]) }
      it { expect(delegate.stderr).to eq([]) }
    end

    context 'with fail_fast and simple command that fails' do
      let(:command) { %Q{bash -c 'exit 1'} }
      let(:options) { {fail_fast: true} }
      it { expect { subject }.to raise_error RuntimeError, "fail_fast: #{command}" }
    end

    context 'when command is interrupted' do
      let(:command) { %Q{bash -c "echo 'test'"} }

      before do
        expect(delegate).to receive(:after_execute) { raise Interrupt }
        subject
      end

      it { expect(delegate.status).to eq(130) }
    end

    context 'with simple command with input' do
      let(:command) { 'cat' }
      let(:input) { "ping\npong" }

      before do
        subject
      end

      it { expect(delegate.status).to eq(0) }
      it { expect(delegate.stdout).to eq(['ping', 'pong']) }
      it { expect(delegate.stderr).to eq([]) }
    end

    context 'with simple command with not-ready input' do
      let(:command) { 'cat' }
      let(:input) { "ping\npong" }

      before do
        expect_any_instance_of(IO).to receive(:wait_writable).and_return(nil)
      end

      it { expect { subject }.to raise_error RuntimeError, /IO stdin not ready/ }
    end
  end
end
