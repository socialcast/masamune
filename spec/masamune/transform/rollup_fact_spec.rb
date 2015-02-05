require 'spec_helper'

describe Masamune::Transform::RollupFact do
  before do
    catalog.schema :postgres do
      dimension 'date', type: :one do
        column 'date_id', type: :integer, unique: true, index: true, natural_key: true
      end

      dimension 'user_agent', type: :mini do
        column 'name', type: :string, unique: true, index: 'shared'
        column 'version', type: :string, unique: true, index: 'shared', default: 'Unknown'
        column 'description', type: :string, null: true, ignore: true
      end

      dimension 'feature', type: :mini do
        column 'name', type: :string, unique: true, index: true
      end

      dimension 'tenant', type: :two do
        column 'tenant_id', type: :integer, index: true, natural_key: true
      end

      dimension 'user', type: :two do
        column 'tenant_id', type: :integer, index: true, natural_key: true
        column 'user_id', type: :integer, index: true, natural_key: true
      end

      fact 'visits', partition: 'y%Ym%m', grain: %w(hourly daily monthly) do
        references :date
        references :tenant
        references :user
        references :user_agent, insert: true
        references :feature, insert: true

        measure 'total', type: :integer, aggregate: :sum
      end
    end
  end

  let(:date) { DateTime.civil(2014,8) }
  let(:source) { catalog.postgres.visits_hourly_fact }
  let(:target) { catalog.postgres.visits_daily_fact }

  context 'with postgres fact' do
    subject(:result) { transform.rollup_fact(source, target, date).to_s }

    it 'should eq render rollup_fact template' do
      is_expected.to eq <<-EOS.strip_heredoc
        BEGIN;

        DROP TABLE IF EXISTS visits_daily_fact_y2014m08_stage CASCADE;
        CREATE TABLE IF NOT EXISTS visits_daily_fact_y2014m08_stage (LIKE visits_daily_fact INCLUDING ALL);

        ALTER TABLE visits_daily_fact_y2014m08_stage ADD CONSTRAINT visits_daily_fact_y2014m08_stage_time_key_check CHECK (time_key >= 1406851200 AND time_key < 1409529600);
        ALTER TABLE visits_daily_fact_y2014m08_stage ADD CONSTRAINT visits_daily_fact_y2014m08_stage_date_dimension_uuid_fkey FOREIGN KEY (date_dimension_uuid) REFERENCES date_dimension(uuid);
        ALTER TABLE visits_daily_fact_y2014m08_stage ADD CONSTRAINT visits_daily_fact_y2014m08_stage_tenant_dimension_uuid_fkey FOREIGN KEY (tenant_dimension_uuid) REFERENCES tenant_dimension(uuid);
        ALTER TABLE visits_daily_fact_y2014m08_stage ADD CONSTRAINT visits_daily_fact_y2014m08_stage_user_dimension_uuid_fkey FOREIGN KEY (user_dimension_uuid) REFERENCES user_dimension(uuid);
        ALTER TABLE visits_daily_fact_y2014m08_stage ADD CONSTRAINT visits_daily_fact_y2014m08_stage_user_agent_type_id_fkey FOREIGN KEY (user_agent_type_id) REFERENCES user_agent_type(id);
        ALTER TABLE visits_daily_fact_y2014m08_stage ADD CONSTRAINT visits_daily_fact_y2014m08_stage_feature_type_id_fkey FOREIGN KEY (feature_type_id) REFERENCES feature_type(id);

        INSERT INTO
          visits_daily_fact_y2014m08_stage (date_dimension_uuid, tenant_dimension_uuid, user_dimension_uuid, user_agent_type_id, feature_type_id, total, time_key)
        SELECT
          visits_hourly_fact_y2014m08.date_dimension_uuid,
          visits_hourly_fact_y2014m08.tenant_dimension_uuid,
          visits_hourly_fact_y2014m08.user_dimension_uuid,
          visits_hourly_fact_y2014m08.user_agent_type_id,
          visits_hourly_fact_y2014m08.feature_type_id,
          SUM(visits_hourly_fact_y2014m08.total),
          MIN(visits_hourly_fact_y2014m08.time_key)
        FROM
          visits_hourly_fact_y2014m08
        GROUP BY
          visits_hourly_fact_y2014m08.date_dimension_uuid,
          visits_hourly_fact_y2014m08.tenant_dimension_uuid,
          visits_hourly_fact_y2014m08.user_dimension_uuid,
          visits_hourly_fact_y2014m08.user_agent_type_id,
          visits_hourly_fact_y2014m08.feature_type_id
        ;

        CREATE INDEX visits_daily_fact_y2014m08_stage_date_dimension_uuid_index ON visits_daily_fact_y2014m08_stage (date_dimension_uuid);
        CREATE INDEX visits_daily_fact_y2014m08_stage_tenant_dimension_uuid_index ON visits_daily_fact_y2014m08_stage (tenant_dimension_uuid);
        CREATE INDEX visits_daily_fact_y2014m08_stage_user_dimension_uuid_index ON visits_daily_fact_y2014m08_stage (user_dimension_uuid);
        CREATE INDEX visits_daily_fact_y2014m08_stage_user_agent_type_id_index ON visits_daily_fact_y2014m08_stage (user_agent_type_id);
        CREATE INDEX visits_daily_fact_y2014m08_stage_feature_type_id_index ON visits_daily_fact_y2014m08_stage (feature_type_id);
        CREATE INDEX visits_daily_fact_y2014m08_stage_time_key_index ON visits_daily_fact_y2014m08_stage (time_key);

        COMMIT;

        BEGIN;

        DROP TABLE IF EXISTS visits_daily_fact_y2014m08;
        ALTER TABLE visits_daily_fact_y2014m08_stage RENAME TO visits_daily_fact_y2014m08;

        ALTER TABLE visits_daily_fact_y2014m08 INHERIT visits_daily_fact;
        ALTER TABLE visits_daily_fact_y2014m08 ADD CONSTRAINT visits_daily_fact_y2014m08_time_key_check CHECK (time_key >= 1406851200 AND time_key < 1409529600) NOT VALID;
        ALTER TABLE visits_daily_fact_y2014m08 ADD CONSTRAINT visits_daily_fact_y2014m08_date_dimension_uuid_fkey FOREIGN KEY (date_dimension_uuid) REFERENCES date_dimension(uuid) NOT VALID;
        ALTER TABLE visits_daily_fact_y2014m08 ADD CONSTRAINT visits_daily_fact_y2014m08_tenant_dimension_uuid_fkey FOREIGN KEY (tenant_dimension_uuid) REFERENCES tenant_dimension(uuid) NOT VALID;
        ALTER TABLE visits_daily_fact_y2014m08 ADD CONSTRAINT visits_daily_fact_y2014m08_user_dimension_uuid_fkey FOREIGN KEY (user_dimension_uuid) REFERENCES user_dimension(uuid) NOT VALID;
        ALTER TABLE visits_daily_fact_y2014m08 ADD CONSTRAINT visits_daily_fact_y2014m08_user_agent_type_id_fkey FOREIGN KEY (user_agent_type_id) REFERENCES user_agent_type(id) NOT VALID;
        ALTER TABLE visits_daily_fact_y2014m08 ADD CONSTRAINT visits_daily_fact_y2014m08_feature_type_id_fkey FOREIGN KEY (feature_type_id) REFERENCES feature_type(id) NOT VALID;

        ALTER INDEX visits_daily_fact_y2014m08_stage_date_dimension_uuid_index RENAME TO visits_daily_fact_y2014m08_date_dimension_uuid_index;
        ALTER INDEX visits_daily_fact_y2014m08_stage_tenant_dimension_uuid_index RENAME TO visits_daily_fact_y2014m08_tenant_dimension_uuid_index;
        ALTER INDEX visits_daily_fact_y2014m08_stage_user_dimension_uuid_index RENAME TO visits_daily_fact_y2014m08_user_dimension_uuid_index;
        ALTER INDEX visits_daily_fact_y2014m08_stage_user_agent_type_id_index RENAME TO visits_daily_fact_y2014m08_user_agent_type_id_index;
        ALTER INDEX visits_daily_fact_y2014m08_stage_feature_type_id_index RENAME TO visits_daily_fact_y2014m08_feature_type_id_index;
        ALTER INDEX visits_daily_fact_y2014m08_stage_time_key_index RENAME TO visits_daily_fact_y2014m08_time_key_index;

        COMMIT;
      EOS
    end
  end
end
