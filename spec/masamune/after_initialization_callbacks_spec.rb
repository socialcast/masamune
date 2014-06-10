require 'spec_helper'

describe Masamune::AfterInitializeCallbacks do
  let(:klass) do
    Class.new do
      include Masamune::AfterInitializeCallbacks

      def first_callback; end
      def early_callback; end
      def default_callback; end
      def unknown_callback; end
      def later_callback; end
      def final_callback; end

      after_initialize(:first) { |o| o.first_callback }
      after_initialize(:early) { |o| o.early_callback }
      after_initialize(:default) { |o| o.default_callback }
      after_initialize(:unknown) { |o| o.unknown_callback }
      after_initialize(:later) { |o| o.later_callback }
      after_initialize(:final) { |o| o.final_callback}
    end
  end

  let(:instance) { klass.new }

  describe '.after_initialize_invoke' do
    before do
      expect(instance).to receive(:first_callback).once.ordered
      expect(instance).to receive(:early_callback).once.ordered
      expect(instance).to receive(:default_callback).once.ordered
      expect(instance).to receive(:unknown_callback).once.ordered
      expect(instance).to receive(:later_callback).once.ordered
      expect(instance).to receive(:final_callback).once.ordered
      instance.after_initialize_invoke
    end

    it 'should call callbacks in priority order' do; end
  end
end
