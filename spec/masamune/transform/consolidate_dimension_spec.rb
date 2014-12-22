require 'spec_helper'
require 'active_support/core_ext/string/strip'

describe Masamune::Transform::ConsolidateDimension do
  let(:environment) { double }
  let(:registry) { Masamune::Schema::Registry.new(environment) }

  before do
    registry.schema do
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

  let(:target) { registry.dimensions[:user] }
  let(:transform) { described_class.new target }

  describe '#consolidate_dimension_as_psql' do
    subject(:result) { transform.consolidate_dimension_as_psql(target.ledger_table, target.stage_table) }

    it 'should eq render consolidate_dimension template' do
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

  describe '#deduplicate_dimension_as_psql' do
    subject(:result) { transform.deduplicate_dimension_as_psql(target.stage_table('consolidated'), target.stage_table('deduplicated')) }

    it 'should eq render deduplicate_dimension template' do
      is_expected.to eq <<-EOS.strip_heredoc
        INSERT INTO
          user_deduplicated_dimension_stage (user_account_state_type_id, tenant_id, user_id, preferences, parent_uuid, record_uuid, start_at)
        SELECT DISTINCT
          user_account_state_type_id,
          tenant_id,
          user_id,
          preferences,
          parent_uuid,
          record_uuid,
          start_at
        FROM (
          SELECT
            user_account_state_type_id,
            tenant_id,
            user_id,
            preferences,
            parent_uuid,
            record_uuid,
            start_at,
            CASE
            WHEN (LAG(user_account_state_type_id) OVER w = user_account_state_type_id) AND (LAG(tenant_id) OVER w = tenant_id) AND (LAG(user_id) OVER w = user_id) AND (LAG(preferences) OVER w = preferences) THEN
              1
            ELSE
              0
            END AS duplicate
          FROM
            user_consolidated_dimension_stage
          WINDOW w AS (PARTITION BY tenant_id, user_id ORDER BY start_at)
        ) tmp
        WHERE
          duplicate = 0
        ;
      EOS
    end
  end

  describe '#relabel_dimension_as_psql' do
    subject(:result) { transform.relabel_dimension_as_psql(target) }

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

  describe '#bulk_upsert_as_psql' do
    subject(:result) { transform.bulk_upsert_as_psql(target.stage_table, target)}

    it 'should eq render bulk_upsert template' do
      is_expected.to eq <<-EOS.strip_heredoc
        BEGIN;
        LOCK TABLE user_dimension IN EXCLUSIVE MODE;

        UPDATE
          user_dimension
        SET
          user_account_state_type_id = user_dimension_stage.user_account_state_type_id,
          preferences = user_dimension_stage.preferences
        FROM
          user_dimension_stage
        WHERE
          user_dimension.tenant_id = user_dimension_stage.tenant_id AND
          user_dimension.user_id = user_dimension_stage.user_id AND
          user_dimension.start_at = user_dimension_stage.start_at
        ;

        INSERT INTO
          user_dimension (user_account_state_type_id, tenant_id, user_id, preferences, parent_uuid, record_uuid, start_at, end_at, version, last_modified_at)
        SELECT
          user_dimension_stage.user_account_state_type_id,
          user_dimension_stage.tenant_id,
          user_dimension_stage.user_id,
          user_dimension_stage.preferences,
          user_dimension_stage.parent_uuid,
          user_dimension_stage.record_uuid,
          user_dimension_stage.start_at,
          user_dimension_stage.end_at,
          user_dimension_stage.version,
          user_dimension_stage.last_modified_at
        FROM
          user_dimension_stage
        LEFT OUTER JOIN
          user_dimension
        ON
          user_dimension.tenant_id = user_dimension_stage.tenant_id AND
          user_dimension.user_id = user_dimension_stage.user_id AND
          user_dimension.start_at = user_dimension_stage.start_at
        WHERE
          user_dimension.tenant_id IS NULL AND
          user_dimension.user_id IS NULL AND
          user_dimension.start_at IS NULL
        ;

        COMMIT;
      EOS
    end
  end
end
