require 'spec_helper'
require 'active_support/core_ext/string/strip'

describe Masamune::Transform::LoadFact do
  let(:date_dimension) do
    Masamune::Schema::Dimension.new id: 'date', type: :one,
      columns: [
        Masamune::Schema::Column.new(id: 'date_id', type: :integer, unique: true, index: true, surrogate_key: true)
      ]
  end

  let(:user_agent_type) do
    Masamune::Schema::Dimension.new id: 'user_agent', type: :mini, insert: true,
      columns: [
        Masamune::Schema::Column.new(id: 'name', type: :string, unique: true, index: 'shared'),
        Masamune::Schema::Column.new(id: 'version', type: :string, unique: true, index: 'shared')
      ]
  end

  let(:feature_type) do
    Masamune::Schema::Dimension.new id: 'feature', type: :mini, insert: true,
      columns: [
        Masamune::Schema::Column.new(id: 'name', type: :string, unique: true, index: true)
      ]
  end

  let(:user_dimension) do
    Masamune::Schema::Dimension.new id: 'user', type: :two,
      columns: [
        Masamune::Schema::Column.new(id: 'tenant_id', type: :integer, index: true, surrogate_key: true),
        Masamune::Schema::Column.new(id: 'user_id', type: :integer, index: true, surrogate_key: true)
      ]
  end

  let(:visit_fact) do
    Masamune::Schema::Fact.new id: 'visits', partition: 'y%Ym%m',
      references: [date_dimension, user_dimension, user_agent_type, feature_type],
      columns: [
        Masamune::Schema::Column.new(id: 'total', type: :integer)
      ]
  end

  let(:data) { (1..3).map { |i| double(path: "output_#{i}.csv") } }
  let(:date) { DateTime.civil(2014,8) }
  let(:target) { visit_fact }
  let(:source) { visit_fact.stage_table(%w(date.date_id user.tenant_id user.user_id user_agent.name user_agent.version feature.name total time_key)) }

  let(:transform) { described_class.new data, source, target, date }

  describe '#stage_fact_as_psql' do
    subject(:result) { transform.stage_fact_as_psql }

    it 'should eq render load_fact template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TEMPORARY TABLE IF NOT EXISTS visits_fact_stage
        (
          date_dimension_date_id INTEGER,
          user_dimension_tenant_id INTEGER,
          user_dimension_user_id INTEGER,
          user_agent_type_name VARCHAR,
          user_agent_type_version VARCHAR,
          feature_type_name VARCHAR,
          total INTEGER,
          time_key INTEGER
        );

        COPY visits_fact_stage FROM 'output_1.csv' WITH (FORMAT 'csv');
        COPY visits_fact_stage FROM 'output_2.csv' WITH (FORMAT 'csv');
        COPY visits_fact_stage FROM 'output_3.csv' WITH (FORMAT 'csv');

        CREATE INDEX visits_fact_stage_date_dimension_date_id_index ON visits_fact_stage (date_dimension_date_id);
        CREATE INDEX visits_fact_stage_user_dimension_tenant_id_index ON visits_fact_stage (user_dimension_tenant_id);
        CREATE INDEX visits_fact_stage_user_dimension_user_id_index ON visits_fact_stage (user_dimension_user_id);
        CREATE INDEX visits_fact_stage_user_agent_type_name_user_agent_type_version_index ON visits_fact_stage (user_agent_type_name, user_agent_type_version);
        CREATE INDEX visits_fact_stage_feature_type_name_index ON visits_fact_stage (feature_type_name);
        CREATE INDEX visits_fact_stage_time_key_index ON visits_fact_stage (time_key);
      EOS
    end
  end

  describe '#load_fact_as_psql' do
    subject(:result) { transform.load_fact_as_psql }

    it 'should eq render template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TABLE visits_fact_y2014m08 (
          CHECK (time_key >= 1406851200 AND time_key < 1409529600)
        ) INHERITS (visits_fact);

        ALTER TABLE visits_fact_y2014m08 ADD CONSTRAINT visits_fact_y2014m08_date_dimension_uuid_fkey FOREIGN KEY (date_dimension_uuid) REFERENCES date_dimension(uuid);
        ALTER TABLE visits_fact_y2014m08 ADD CONSTRAINT visits_fact_y2014m08_user_dimension_uuid_fkey FOREIGN KEY (user_dimension_uuid) REFERENCES user_dimension(uuid);
        ALTER TABLE visits_fact_y2014m08 ADD CONSTRAINT visits_fact_y2014m08_user_agent_type_id_fkey FOREIGN KEY (user_agent_type_id) REFERENCES user_agent_type(id);
        ALTER TABLE visits_fact_y2014m08 ADD CONSTRAINT visits_fact_y2014m08_feature_type_id_fkey FOREIGN KEY (feature_type_id) REFERENCES feature_type(id);

        INSERT INTO
          visits_fact_y2014m08 (date_dimension_uuid, user_dimension_uuid, user_agent_type_id, feature_type_id, total, time_key)
        SELECT
          date_dimension.uuid,
          user_dimension.uuid,
          user_agent_type.id,
          feature_type.id,
          visits_fact_stage.total,
          visits_fact_stage.time_key
        FROM
          visits_fact_stage
        JOIN
          date_dimension
        ON
          date_dimension.date_id = visits_fact_stage.date_dimension_date_id
        JOIN
          user_dimension
        ON
          user_dimension.tenant_id = visits_fact_stage.user_dimension_tenant_id AND
          user_dimension.user_id = visits_fact_stage.user_dimension_user_id AND
          TO_TIMESTAMP(visits_fact_stage.time_key) BETWEEN user_dimension.start_at AND COALESCE(user_dimension.end_at, 'INFINITY')
        JOIN
          user_agent_type
        ON
          user_agent_type.name = visits_fact_stage.user_agent_type_name AND
          user_agent_type.version = visits_fact_stage.user_agent_type_version
        JOIN
          feature_type
        ON
          feature_type.name = visits_fact_stage.feature_type_name
        ;
      EOS
    end
  end

  describe '#insert_reference_values_as_psql' do
    subject(:result) { transform.insert_reference_values_as_psql }

    it 'should eq render template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TEMPORARY TABLE IF NOT EXISTS user_agent_type_stage (LIKE user_agent_type INCLUDING ALL);

        INSERT INTO user_agent_type_stage(name, version)
        SELECT DISTINCT
          user_agent_type_name, user_agent_type_version
        FROM
          visits_fact_stage
        WHERE
          user_agent_type_name IS NOT NULL AND user_agent_type_version IS NOT NULL
        ;

        BEGIN;
        LOCK TABLE user_agent_type IN EXCLUSIVE MODE;

        INSERT INTO
          user_agent_type (name,version)
        SELECT
          user_agent_type_stage.name,
          user_agent_type_stage.version
        FROM
          user_agent_type_stage
        LEFT OUTER JOIN
          user_agent_type
        ON
          user_agent_type.name = user_agent_type_stage.name AND
          user_agent_type.version = user_agent_type_stage.version
        WHERE
          user_agent_type.name IS NULL AND
          user_agent_type.version IS NULL
        ;

        COMMIT;

        CREATE TEMPORARY TABLE IF NOT EXISTS feature_type_stage (LIKE feature_type INCLUDING ALL);

        INSERT INTO feature_type_stage(name)
        SELECT DISTINCT
          feature_type_name
        FROM
          visits_fact_stage
        WHERE
          feature_type_name IS NOT NULL
        ;

        BEGIN;
        LOCK TABLE feature_type IN EXCLUSIVE MODE;

        INSERT INTO
          feature_type (name)
        SELECT
          feature_type_stage.name
        FROM
          feature_type_stage
        LEFT OUTER JOIN
          feature_type
        ON
          feature_type.name = feature_type_stage.name
        WHERE
          feature_type.name IS NULL
        ;

        COMMIT;
      EOS
    end
  end
end
