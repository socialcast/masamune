require 'spec_helper'
require 'active_support/core_ext/string/strip'

describe Masamune::Transform::ConsolidateDimension do
  let(:mini_dimension) do
    Masamune::Schema::Dimension.new name: 'user_account_state', type: :mini,
      columns: [
        Masamune::Schema::Column.new(name: 'name', type: :string, unique: true),
        Masamune::Schema::Column.new(name: 'description', type: :string)
      ]
  end

  let(:dimension) do
    Masamune::Schema::Dimension.new name: 'user', ledger: true,
      references: [mini_dimension],
      columns: [
        Masamune::Schema::Column.new(name: 'tenant_id', index: true, surrogate_key: true),
        Masamune::Schema::Column.new(name: 'user_id', index: true, surrogate_key: true),
        Masamune::Schema::Column.new(name: 'preferences', type: :key_value, null: true)
      ]
  end

  let(:transform) { described_class.new dimension }

  describe '#consolidate_dimension_as_psql' do
    subject(:result) { transform.consolidate_dimension_as_psql }

    it 'should eq render consolidate_dimension template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TEMPORARY TABLE IF NOT EXISTS user_stage (LIKE user_dimension INCLUDING ALL);

        WITH ranges AS (
          SELECT *,
          CASE WHEN delta = 0
          THEN 1 ELSE NULL END r
          FROM user_dimension_ledger
        ), windows AS (
          SELECT *,
          SUM(r) OVER (ORDER BY tenant_id, user_id, start_at) window_id
          FROM ranges
        )
        INSERT INTO
          user_stage (user_account_state_type_id,tenant_id,user_id,preferences,parent_uuid,record_uuid,start_at)
        SELECT
          consolidated.user_account_state_type_id,
          consolidated.tenant_id,
          consolidated.user_id,
          consolidated.preferences,
          consolidated.parent_uuid,
          consolidated.record_uuid,
          consolidated.start_at
        FROM (
          SELECT
            FIRST_VALUE(uuid) OVER w AS parent_uuid,
            FIRST_VALUE(start_at) OVER w AS parent_start_at,
            uuid AS record_uuid,
            COALESCE(user_account_state_type_id, FIRST_VALUE(user_account_state_type_id) OVER w) AS user_account_state_type_id,
            tenant_id AS tenant_id,
            user_id AS user_id,
            hstore_merge(preferences_now) OVER w - hstore_merge(preferences_was) OVER w AS preferences,
            start_at AS start_at
          FROM
            windows
          WINDOW w AS (PARTITION BY tenant_id, user_id, window_id ORDER BY start_at)
        ) consolidated
        WHERE
          consolidated.user_account_state_type_id IS NOT NULL AND
          consolidated.tenant_id IS NOT NULL AND
          consolidated.user_id IS NOT NULL AND
          (
            parent_uuid = record_uuid OR
            parent_start_at <> start_at
          )
        ;
      EOS
    end
  end

  describe '#relabel_dimension_as_psql' do
    subject(:result) { transform.relabel_dimension_as_psql }

    it 'should eq render relabel_dimension template' do
      is_expected.to eq <<-EOS.strip_heredoc
        BEGIN;
        LOCK TABLE user_dimension IN EXCLUSIVE MODE;

        UPDATE user_dimension SET version = NULL;

        UPDATE
          user_dimension
        SET
          version = tmp.version
        FROM
          (
            SELECT
              uuid,
              tenant_id,
              user_id,
              start_at,
              rank() OVER (PARTITION BY tenant_id, user_id ORDER BY start_at) AS version
            FROM
              user_dimension
            GROUP BY
              uuid, tenant_id, user_id, start_at
           ) AS tmp
        WHERE
          user_dimension.uuid = tmp.uuid
        ;

        UPDATE user_dimension SET end_at = NULL;

        UPDATE
          user_dimension
        SET
          end_at = tmp.end_at
        FROM
          (
            SELECT
              uuid,
              start_at,
              tenant_id,
              user_id,
              LEAD(start_at, 1) OVER (PARTITION BY tenant_id, user_id ORDER BY start_at) AS end_at
            FROM
              user_dimension
            GROUP BY
              uuid, tenant_id, user_id, start_at
           ) AS tmp
        WHERE
          user_dimension.uuid = tmp.uuid
        ;

        COMMIT;
      EOS
    end
  end
end
