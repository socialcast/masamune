require 'spec_helper'
require 'active_support/core_ext/string/strip'

describe Masamune::Transform::DefineEventView do
  let(:environment) { double }
  let(:registry) { Masamune::Schema::Registry.new(environment) }

  before do
    registry.schema do
      event 'tenant' do
        attribute 'tenant_id', type: :integer, immutable: true
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
        CREATE VIEW IF NOT EXISTS tenant_events (
          uuid,
          type,
          tenant_id,
          account_state_now,
          account_state_was,
          premium_type_now,
          premium_type_was,
          preferences_now,
          preferences_was,
          delta,
          created_at,
          y, m, d ,h
        ) PARTITIONED ON (y, m, d, h) AS
        SELECT DISTINCT
          uuid,
          type,
          tenant_id,
          account_state_now,
          account_state_was,
          premium_type_now,
          premium_type_was,
          preferences_now,
          preferences_was,
          IF(type = 'tenant_update', 1, 0) AS delta,
          ctime_iso8601 AS created_at,
          y, m, d ,h
        FROM
          events
        LATERAL VIEW
          json_tuple(events.json, 'tenant_id', 'account_state_now', 'account_state_was', 'premium_type_now', 'premium_type_was', 'preferences_now', 'preferences_was') event_data AS tenant_id, account_state_now, account_state_was, premium_type_now, premium_type_was, preferences_now, preferences_was
        WHERE
          type = 'tenant_create' OR
          type = 'tenant_update' OR
          type = 'tenant_delete'
        SORT BY
          created_at
        ;
      EOS
    end
  end
end
