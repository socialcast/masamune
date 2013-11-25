require 'spec_helper'

describe Masamune::Actions::Execute do
  let(:klass) do
    Class.new do
      include Masamune::HasContext
      include Masamune::Actions::Execute
    end
  end

  let(:instance) { klass.new }

  describe '.execute' do
    context 'with a simple command' do
      let(:command) { %w(echo ping) }
      let(:options) { {fail_fast: true} }

      it { expect { |b| instance.execute(*command, options, &b) }.to yield_with_args('ping', 0) }
    end

    context 'with a simple command with input' do
      let(:command) { %w(cat) }
      let(:options) { {input: 'pong', fail_fast: true} }

      it { expect { |b| instance.execute(*command, options, &b) }.to yield_with_args('pong', 0) }
    end
  end
end
