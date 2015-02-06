require 'spec_helper'

describe Masamune::Transform::SnapshotDimension do
  before do
    catalog.schema :postgres do
      dimension 'user_account_state', type: :mini do
        column 'name', type: :string, unique: true
        column 'description', type: :string
      end

      dimension 'user', type: :four do
        references :user_account_state
        column 'tenant_id', index: true, natural_key: true
        column 'user_id', index: true, natural_key: true
        column 'preferences', type: :key_value, null: true
      end
    end
  end

  let(:target) { catalog.postgres.user_dimension }

  context 'with postgres dimension' do
    subject(:result) { transform.snapshot_dimension(target.ledger_table, target.stage_table).to_s }

    it 'should eq render snapshot_dimension template' do
      is_expected.to eq <<-EOS.strip_heredoc
        WITH ranges AS (
          SELECT *,
          CASE WHEN delta = 0
          THEN 1 ELSE NULL END r
          FROM user_dimension_ledger
        ), windows AS (
          SELECT *,
          SUM(r) OVER (ORDER BY tenant_id, user_id, start_at DESC, delta, source_uuid) window_id
          FROM ranges
        ), snapshot AS (
          SELECT
            consolidated.user_account_state_type_id,
            consolidated.tenant_id,
            consolidated.user_id,
            consolidated.preferences,
            consolidated.parent_uuid,
            consolidated.record_uuid,
            consolidated.start_at
          FROM (
            SELECT DISTINCT ON (tenant_id, user_id, start_at)
              FIRST_VALUE(uuid) OVER w AS parent_uuid,
              FIRST_VALUE(start_at) OVER w AS parent_start_at,
              uuid AS record_uuid,
              coalesce_merge(user_account_state_type_id) OVER w AS user_account_state_type_id,
              tenant_id AS tenant_id,
              user_id AS user_id,
              hstore_merge(preferences_now) OVER w - hstore_merge(preferences_was) OVER w AS preferences,
              start_at AS start_at
            FROM
              windows
            WINDOW w AS (PARTITION BY tenant_id, user_id, window_id ORDER BY start_at DESC)
            ORDER BY tenant_id, user_id, start_at DESC, window_id
          ) consolidated
          WHERE
            consolidated.user_account_state_type_id IS NOT NULL AND
            consolidated.tenant_id IS NOT NULL AND
            consolidated.user_id IS NOT NULL
        )
        INSERT INTO
          user_dimension_stage (user_account_state_type_id, tenant_id, user_id, preferences, parent_uuid, record_uuid, start_at)
        SELECT
          user_account_state_type_id,
          tenant_id,
          user_id,
          preferences,
          parent_uuid,
          record_uuid,
          start_at
        FROM
          snapshot
        ;
      EOS
    end
  end
end
