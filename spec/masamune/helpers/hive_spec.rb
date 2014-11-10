require 'spec_helper'

describe Masamune::Helpers::Hive do
  let(:environment) { double }
  let(:instance) { described_class.new(environment) }

  describe '#drop_partition' do
    before do
      expect(instance).to receive(:hive).with(exec: 'ALTER TABLE foo DROP PARTITION (y=2014,m=10);', fail_fast: true).and_return(mock_success)
      instance.drop_partition('foo', 'y=2014,m=10')
    end

    it 'meets expectations' do; end
  end
end
