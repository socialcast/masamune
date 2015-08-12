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

describe Masamune::Transform::StageFact do
  before do
    catalog.schema :postgres do
      dimension 'cluster', type: :mini do
        column 'id', type: :sequence, surrogate_key: true, auto: true
        column 'name', type: :string

        row name: 'current_database()', attributes: {default: true}
      end

      dimension 'date', type: :date do
        column 'date_id', type: :integer, unique: true, index: true, natural_key: true
      end

      dimension 'user_agent', type: :mini do
        column 'name', type: :string, unique: true, index: 'shared'
        column 'version', type: :string, unique: true, index: 'shared', default: 'Unknown'
        column 'mobile', type: :boolean, unique: true, index: 'shared', default: false
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

      dimension 'group', type: :two do
        column 'group_id', type: :integer, index: true, natural_key: true
        column 'group_mode', type: :enum, sub_type: 'group_mode', values: %(missing public private), index: true, natural_key: true, default: 'missing'
        row group_id: -1, group_mode: 'missing', attributes: {id: :missing}
      end

      fact 'visits', partition: 'y%Ym%m', grain: %w(hourly daily monthly) do
        references :cluster
        references :date
        references :tenant
        references :user
        references :group, default: :missing
        references :user_agent, insert: true
        references :feature, insert: true
        references :session, degenerate: true
        measure 'total', type: :integer
      end

      file 'visits_hourly' do
        column 'date.date_id', type: :integer
        column 'tenant.tenant_id', type: :integer
        column 'user.user_id', type: :integer
        column 'group.group_id', type: :integer
        column 'group.group_mode', type: :enum, sub_type: 'group_mode'
        column 'user_agent.name', type: :string
        column 'user_agent.version', type: :string
        column 'user_agent.mobile', type: :boolean
        column 'feature.name', type: :string
        column 'session.id', type: :integer
        column 'time_key', type: :integer
        column 'total', type: :integer
      end
    end
  end

  let(:date) { DateTime.civil(2014,8) }
  let(:target) { catalog.postgres.visits_hourly_fact }
  let(:source) { catalog.postgres.visits_hourly_file.stage_table(suffix: 'file', table: target, inherit: false) }

  context 'with postgres fact' do
    subject(:result) { transform.stage_fact(source, target, date).to_s }

    it 'should eq render stage_fact template' do
      is_expected.to eq <<-EOS.strip_heredoc
        BEGIN;

        DROP TABLE IF EXISTS visits_hourly_fact_y2014m08_stage CASCADE;
        CREATE TABLE IF NOT EXISTS visits_hourly_fact_y2014m08_stage (LIKE visits_hourly_fact INCLUDING ALL);

        ALTER TABLE visits_hourly_fact_y2014m08_stage ADD CONSTRAINT visits_hourly_fact_y2014m08_stage_time_key_check CHECK (time_key >= 1406851200 AND time_key < 1409529600);

        INSERT INTO
          visits_hourly_fact_y2014m08_stage (date_dimension_id, tenant_dimension_id, user_dimension_id, group_dimension_id, user_agent_type_id, feature_type_id, session_type_id, total, time_key)
        SELECT
          date_dimension.id,
          tenant_dimension.id,
          user_dimension.id,
          group_dimension.id,
          user_agent_type.id,
          feature_type.id,
          visits_hourly_file_fact_stage.session_type_id,
          visits_hourly_file_fact_stage.total,
          visits_hourly_file_fact_stage.time_key
        FROM
          visits_hourly_file_fact_stage
        JOIN
          date_dimension
        ON
          date_dimension.date_id = visits_hourly_file_fact_stage.date_dimension_date_id
        JOIN
          user_dimension
        ON
          user_dimension.user_id = visits_hourly_file_fact_stage.user_dimension_user_id AND
          ((TO_TIMESTAMP(visits_hourly_file_fact_stage.time_key) BETWEEN user_dimension.start_at AND COALESCE(user_dimension.end_at, 'INFINITY')) OR (TO_TIMESTAMP(visits_hourly_file_fact_stage.time_key) < user_dimension.start_at AND user_dimension.version = 1))
        JOIN
          tenant_dimension
        ON
          tenant_dimension.tenant_id = COALESCE(visits_hourly_file_fact_stage.tenant_dimension_tenant_id, user_dimension.tenant_id) AND
          ((TO_TIMESTAMP(visits_hourly_file_fact_stage.time_key) BETWEEN tenant_dimension.start_at AND COALESCE(tenant_dimension.end_at, 'INFINITY')) OR (TO_TIMESTAMP(visits_hourly_file_fact_stage.time_key) < tenant_dimension.start_at AND tenant_dimension.version = 1))
        JOIN
          group_dimension
        ON
          group_dimension.group_id = COALESCE(visits_hourly_file_fact_stage.group_dimension_group_id, missing_group_id()) AND
          group_dimension.group_mode = COALESCE(visits_hourly_file_fact_stage.group_dimension_group_mode, missing_group_mode()) AND
          ((TO_TIMESTAMP(visits_hourly_file_fact_stage.time_key) BETWEEN group_dimension.start_at AND COALESCE(group_dimension.end_at, 'INFINITY')) OR (TO_TIMESTAMP(visits_hourly_file_fact_stage.time_key) < group_dimension.start_at AND group_dimension.version = 1))
        JOIN
          user_agent_type
        ON
          user_agent_type.name = visits_hourly_file_fact_stage.user_agent_type_name AND
          user_agent_type.version = COALESCE(visits_hourly_file_fact_stage.user_agent_type_version, 'Unknown') AND
          user_agent_type.mobile = COALESCE(visits_hourly_file_fact_stage.user_agent_type_mobile, FALSE)
        JOIN
          feature_type
        ON
          feature_type.name = visits_hourly_file_fact_stage.feature_type_name
        ;

        DROP TABLE IF EXISTS visits_hourly_fact_y2014m08 CASCADE;
        ALTER TABLE visits_hourly_fact_y2014m08_stage RENAME TO visits_hourly_fact_y2014m08;
        ALTER TABLE visits_hourly_fact_y2014m08 DROP CONSTRAINT visits_hourly_fact_y2014m08_stage_time_key_check;

        ALTER TABLE visits_hourly_fact_y2014m08 INHERIT visits_hourly_fact;
        ALTER TABLE visits_hourly_fact_y2014m08 ADD CONSTRAINT visits_hourly_fact_y2014m08_time_key_check CHECK (time_key >= 1406851200 AND time_key < 1409529600) NOT VALID;
        ALTER TABLE visits_hourly_fact_y2014m08 ADD CONSTRAINT visits_hourly_fact_y2014m08_cluster_type_id_fkey FOREIGN KEY (cluster_type_id) REFERENCES cluster_type(id) NOT VALID DEFERRABLE INITIALLY DEFERRED;
        ALTER TABLE visits_hourly_fact_y2014m08 ADD CONSTRAINT visits_hourly_fact_y2014m08_date_dimension_id_fkey FOREIGN KEY (date_dimension_id) REFERENCES date_dimension(id) NOT VALID DEFERRABLE INITIALLY DEFERRED;
        ALTER TABLE visits_hourly_fact_y2014m08 ADD CONSTRAINT visits_hourly_fact_y2014m08_tenant_dimension_id_fkey FOREIGN KEY (tenant_dimension_id) REFERENCES tenant_dimension(id) NOT VALID DEFERRABLE INITIALLY DEFERRED;
        ALTER TABLE visits_hourly_fact_y2014m08 ADD CONSTRAINT visits_hourly_fact_y2014m08_user_dimension_id_fkey FOREIGN KEY (user_dimension_id) REFERENCES user_dimension(id) NOT VALID DEFERRABLE INITIALLY DEFERRED;
        ALTER TABLE visits_hourly_fact_y2014m08 ADD CONSTRAINT visits_hourly_fact_y2014m08_group_dimension_id_fkey FOREIGN KEY (group_dimension_id) REFERENCES group_dimension(id) NOT VALID DEFERRABLE INITIALLY DEFERRED;
        ALTER TABLE visits_hourly_fact_y2014m08 ADD CONSTRAINT visits_hourly_fact_y2014m08_user_agent_type_id_fkey FOREIGN KEY (user_agent_type_id) REFERENCES user_agent_type(id) NOT VALID DEFERRABLE INITIALLY DEFERRED;
        ALTER TABLE visits_hourly_fact_y2014m08 ADD CONSTRAINT visits_hourly_fact_y2014m08_feature_type_id_fkey FOREIGN KEY (feature_type_id) REFERENCES feature_type(id) NOT VALID DEFERRABLE INITIALLY DEFERRED;

        CREATE INDEX visits_hourly_fact_y2014m08_d6b9b38_index ON visits_hourly_fact (cluster_type_id);
        CREATE INDEX visits_hourly_fact_y2014m08_0a531a8_index ON visits_hourly_fact (date_dimension_id);
        CREATE INDEX visits_hourly_fact_y2014m08_d3950d9_index ON visits_hourly_fact (tenant_dimension_id);
        CREATE INDEX visits_hourly_fact_y2014m08_39f0fdd_index ON visits_hourly_fact (user_dimension_id);
        CREATE INDEX visits_hourly_fact_y2014m08_e0d2a9e_index ON visits_hourly_fact (group_dimension_id);
        CREATE INDEX visits_hourly_fact_y2014m08_d8b1c3e_index ON visits_hourly_fact (user_agent_type_id);
        CREATE INDEX visits_hourly_fact_y2014m08_33b68fd_index ON visits_hourly_fact (feature_type_id);
        CREATE INDEX visits_hourly_fact_y2014m08_422efee_index ON visits_hourly_fact (session_type_id);
        CREATE INDEX visits_hourly_fact_y2014m08_6444ed3_index ON visits_hourly_fact (time_key);

        ANALYZE visits_hourly_fact_y2014m08;

        COMMIT;
      EOS
    end
  end
end
