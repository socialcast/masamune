#  The MIT License (MIT)
#
#  Copyright (c) 2014-2015, VMware, Inc. All Rights Reserved.
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in
#  all copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
#  THE SOFTWARE.

require 'spec_helper'

describe Masamune::Transform::RollupFact do
  before do
    catalog.schema :postgres do
      dimension 'date', type: :date do
        column 'date_id', type: :integer, unique: true, index: true, natural_key: true
        column 'date_epoch', type: :integer
        column 'month_epoch', type: :integer
        column 'year_epoch', type: :integer
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

  context 'with postgres hourly fact' do
    let(:date) { DateTime.civil(2014,8) }
    let(:source) { catalog.postgres.visits_hourly_fact }
    let(:target) { catalog.postgres.visits_daily_fact }

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
          (SELECT uuid FROM date_dimension d WHERE d.date_epoch = date_dimension.date_epoch ORDER BY d.date_id LIMIT 1),
          visits_hourly_fact_y2014m08.tenant_dimension_uuid,
          visits_hourly_fact_y2014m08.user_dimension_uuid,
          visits_hourly_fact_y2014m08.user_agent_type_id,
          visits_hourly_fact_y2014m08.feature_type_id,
          SUM(visits_hourly_fact_y2014m08.total),
          (SELECT date_epoch FROM date_dimension d WHERE d.date_epoch = date_dimension.date_epoch ORDER BY d.date_id LIMIT 1)
        FROM
          visits_hourly_fact_y2014m08
        JOIN
          date_dimension
        ON
          date_dimension.uuid = visits_hourly_fact_y2014m08.date_dimension_uuid
        GROUP BY
          date_dimension.date_epoch,
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

  context 'with postgres daily fact' do
    let(:date) { DateTime.civil(2014,8) }
    let(:source) { catalog.postgres.visits_daily_fact }
    let(:target) { catalog.postgres.visits_monthly_fact }

    subject(:result) { transform.rollup_fact(source, target, date).to_s }

    it 'should eq render rollup_fact template' do
      is_expected.to eq <<-EOS.strip_heredoc
        BEGIN;

        DROP TABLE IF EXISTS visits_monthly_fact_y2014m08_stage CASCADE;
        CREATE TABLE IF NOT EXISTS visits_monthly_fact_y2014m08_stage (LIKE visits_monthly_fact INCLUDING ALL);

        ALTER TABLE visits_monthly_fact_y2014m08_stage ADD CONSTRAINT visits_monthly_fact_y2014m08_stage_time_key_check CHECK (time_key >= 1406851200 AND time_key < 1409529600);
        ALTER TABLE visits_monthly_fact_y2014m08_stage ADD CONSTRAINT visits_monthly_fact_y2014m08_stage_date_dimension_uuid_fkey FOREIGN KEY (date_dimension_uuid) REFERENCES date_dimension(uuid);
        ALTER TABLE visits_monthly_fact_y2014m08_stage ADD CONSTRAINT visits_monthly_fact_y2014m08_stage_tenant_dimension_uuid_fkey FOREIGN KEY (tenant_dimension_uuid) REFERENCES tenant_dimension(uuid);
        ALTER TABLE visits_monthly_fact_y2014m08_stage ADD CONSTRAINT visits_monthly_fact_y2014m08_stage_user_dimension_uuid_fkey FOREIGN KEY (user_dimension_uuid) REFERENCES user_dimension(uuid);
        ALTER TABLE visits_monthly_fact_y2014m08_stage ADD CONSTRAINT visits_monthly_fact_y2014m08_stage_user_agent_type_id_fkey FOREIGN KEY (user_agent_type_id) REFERENCES user_agent_type(id);
        ALTER TABLE visits_monthly_fact_y2014m08_stage ADD CONSTRAINT visits_monthly_fact_y2014m08_stage_feature_type_id_fkey FOREIGN KEY (feature_type_id) REFERENCES feature_type(id);

        INSERT INTO
          visits_monthly_fact_y2014m08_stage (date_dimension_uuid, tenant_dimension_uuid, user_dimension_uuid, user_agent_type_id, feature_type_id, total, time_key)
        SELECT
          (SELECT uuid FROM date_dimension d WHERE d.month_epoch = date_dimension.month_epoch ORDER BY d.date_id LIMIT 1),
          visits_daily_fact_y2014m08.tenant_dimension_uuid,
          visits_daily_fact_y2014m08.user_dimension_uuid,
          visits_daily_fact_y2014m08.user_agent_type_id,
          visits_daily_fact_y2014m08.feature_type_id,
          SUM(visits_daily_fact_y2014m08.total),
          (SELECT month_epoch FROM date_dimension d WHERE d.month_epoch = date_dimension.month_epoch ORDER BY d.date_id LIMIT 1)
        FROM
          visits_daily_fact_y2014m08
        JOIN
          date_dimension
        ON
          date_dimension.uuid = visits_daily_fact_y2014m08.date_dimension_uuid
        GROUP BY
          date_dimension.month_epoch,
          visits_daily_fact_y2014m08.tenant_dimension_uuid,
          visits_daily_fact_y2014m08.user_dimension_uuid,
          visits_daily_fact_y2014m08.user_agent_type_id,
          visits_daily_fact_y2014m08.feature_type_id
        ;

        CREATE INDEX visits_monthly_fact_y2014m08_stage_date_dimension_uuid_index ON visits_monthly_fact_y2014m08_stage (date_dimension_uuid);
        CREATE INDEX visits_monthly_fact_y2014m08_stage_tenant_dimension_uuid_index ON visits_monthly_fact_y2014m08_stage (tenant_dimension_uuid);
        CREATE INDEX visits_monthly_fact_y2014m08_stage_user_dimension_uuid_index ON visits_monthly_fact_y2014m08_stage (user_dimension_uuid);
        CREATE INDEX visits_monthly_fact_y2014m08_stage_user_agent_type_id_index ON visits_monthly_fact_y2014m08_stage (user_agent_type_id);
        CREATE INDEX visits_monthly_fact_y2014m08_stage_feature_type_id_index ON visits_monthly_fact_y2014m08_stage (feature_type_id);
        CREATE INDEX visits_monthly_fact_y2014m08_stage_time_key_index ON visits_monthly_fact_y2014m08_stage (time_key);

        COMMIT;

        BEGIN;

        DROP TABLE IF EXISTS visits_monthly_fact_y2014m08;
        ALTER TABLE visits_monthly_fact_y2014m08_stage RENAME TO visits_monthly_fact_y2014m08;

        ALTER TABLE visits_monthly_fact_y2014m08 INHERIT visits_monthly_fact;
        ALTER TABLE visits_monthly_fact_y2014m08 ADD CONSTRAINT visits_monthly_fact_y2014m08_time_key_check CHECK (time_key >= 1406851200 AND time_key < 1409529600) NOT VALID;
        ALTER TABLE visits_monthly_fact_y2014m08 ADD CONSTRAINT visits_monthly_fact_y2014m08_date_dimension_uuid_fkey FOREIGN KEY (date_dimension_uuid) REFERENCES date_dimension(uuid) NOT VALID;
        ALTER TABLE visits_monthly_fact_y2014m08 ADD CONSTRAINT visits_monthly_fact_y2014m08_tenant_dimension_uuid_fkey FOREIGN KEY (tenant_dimension_uuid) REFERENCES tenant_dimension(uuid) NOT VALID;
        ALTER TABLE visits_monthly_fact_y2014m08 ADD CONSTRAINT visits_monthly_fact_y2014m08_user_dimension_uuid_fkey FOREIGN KEY (user_dimension_uuid) REFERENCES user_dimension(uuid) NOT VALID;
        ALTER TABLE visits_monthly_fact_y2014m08 ADD CONSTRAINT visits_monthly_fact_y2014m08_user_agent_type_id_fkey FOREIGN KEY (user_agent_type_id) REFERENCES user_agent_type(id) NOT VALID;
        ALTER TABLE visits_monthly_fact_y2014m08 ADD CONSTRAINT visits_monthly_fact_y2014m08_feature_type_id_fkey FOREIGN KEY (feature_type_id) REFERENCES feature_type(id) NOT VALID;

        ALTER INDEX visits_monthly_fact_y2014m08_stage_date_dimension_uuid_index RENAME TO visits_monthly_fact_y2014m08_date_dimension_uuid_index;
        ALTER INDEX visits_monthly_fact_y2014m08_stage_tenant_dimension_uuid_index RENAME TO visits_monthly_fact_y2014m08_tenant_dimension_uuid_index;
        ALTER INDEX visits_monthly_fact_y2014m08_stage_user_dimension_uuid_index RENAME TO visits_monthly_fact_y2014m08_user_dimension_uuid_index;
        ALTER INDEX visits_monthly_fact_y2014m08_stage_user_agent_type_id_index RENAME TO visits_monthly_fact_y2014m08_user_agent_type_id_index;
        ALTER INDEX visits_monthly_fact_y2014m08_stage_feature_type_id_index RENAME TO visits_monthly_fact_y2014m08_feature_type_id_index;
        ALTER INDEX visits_monthly_fact_y2014m08_stage_time_key_index RENAME TO visits_monthly_fact_y2014m08_time_key_index;

        COMMIT;
      EOS
    end
  end
end
