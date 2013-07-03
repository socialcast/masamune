require 'spec_helper'

describe Masamune::Commands::Shell do
  let(:delegate) { MockDelegate.new(command) }
  let(:instance) { described_class.new(delegate) }

  context 'with simple command' do
    let(:command) { %Q{echo 'stdout 1'; echo 'stderr 1' 1>&2; echo 'stdout 2'; echo 'stderr 2' 1>&2} }

    before do
      instance.execute
    end

    it { delegate.stdout.should == ['stdout 1', 'stdout 2'] }
    it { delegate.stderr.should == ['stderr 1', 'stderr 2'] }
  end
end
