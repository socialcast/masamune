require 'spec_helper'
require 'active_support/core_ext/string/strip'

describe Masamune::Transform::DefineEventView do
  let(:environment) { double }
  let(:registry) { Masamune::Schema::Registry.new(environment) }

  before do
    registry.schema do
      event 'tenant' do
        attribute 'tenant_id', type: :integer
        attribute 'account_state', type: :string
        attribute 'premium_type', type: :string
        attribute 'preferences', type: :json
      end
    end
  end

  let(:target) { registry.events[:tenant] }
  # FIXME view is derived from events store
  let(:source) { nil }

  let(:transform) { described_class.new source, target }

  describe '#as_hql' do
    subject(:result) { transform.as_hql }

    it 'should eq render template' do
      is_expected.to eq <<-EOS.strip_heredoc

      EOS
    end
  end
end
