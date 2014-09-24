require 'spec_helper'
require 'active_support/core_ext/string/strip'

describe Masamune::Schema::Event do
  context 'with columns' do
    let(:event) do
      described_class.new id: 'user',
        columns: [
          Masamune::Schema::Column.new(id: 'tenant_id'),
          Masamune::Schema::Column.new(id: 'user_id')
        ]
    end

    it { expect(event.columns).to include :tenant_id }
    it { expect(event.columns).to include :user_id }
  end
end

