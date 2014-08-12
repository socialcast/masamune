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
        Masamune::Schema::Column.new(id: 'name', type: :string, unique: 'shared', index: 'shared'),
        Masamune::Schema::Column.new(id: 'version', type: :string, unique: 'shared', index: 'shared')
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
    Masamune::Schema::Fact.new id: 'visits',
      references: [date_dimension, user_dimension, user_agent_type],
      columns: [
        Masamune::Schema::Column.new(id: 'time_unix', type: :integer, index: true),
        Masamune::Schema::Column.new(id: 'total', type: :integer)
      ]
  end

  let(:target) { visit_fact }
  let(:source) { visit_fact.stage_table(%w(date.date_id user.tenant_id user.user_id user_agent.name user_agent.version time_unix total)) }

  describe '#shared_columns' do
    subject(:result) { target.shared_columns(source) }

    it do
      is_expected.to_not be_empty
    end
  end

  let(:transform) { described_class.new ['output_1.csv', 'output_2.csv', 'output_3.csv'], source, target }

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
          time_unix INTEGER,
          total INTEGER
        );

        COPY visits_fact_stage FROM 'output_1.csv' WITH (FORMAT 'csv');
        COPY visits_fact_stage FROM 'output_2.csv' WITH (FORMAT 'csv');
        COPY visits_fact_stage FROM 'output_3.csv' WITH (FORMAT 'csv');

        CREATE INDEX visits_fact_stage_date_dimension_date_id_index ON visits_fact_stage (date_dimension_date_id);
        CREATE INDEX visits_fact_stage_user_dimension_tenant_id_index ON visits_fact_stage (user_dimension_tenant_id);
        CREATE INDEX visits_fact_stage_user_dimension_user_id_index ON visits_fact_stage (user_dimension_user_id);
        CREATE INDEX visits_fact_stage_user_agent_type_name_user_agent_type_version_index ON visits_fact_stage (user_agent_type_name, user_agent_type_version);
        CREATE INDEX visits_fact_stage_time_unix_index ON visits_fact_stage (time_unix);
      EOS
    end
  end

  describe '#load_fact_as_psql' do
    subject(:result) { transform.load_fact_as_psql }

    it 'should eq render template' do
      is_expected.to eq <<-EOS.strip_heredoc
        INSERT INTO
          visits_fact (date_dimension_uuid, user_dimension_uuid, user_agent_type_id, time_unix, total)
        SELECT
          date_dimension.uuid,
          user_dimension.uuid,
          user_agent_type.id,
          visits_fact_stage.time_unix,
          visits_fact_stage.total
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
          TO_TIMESTAMP(visits_fact_stage.time_unix)::DATE BETWEEN user_dimension.start_at::DATE AND COALESCE(user_dimension.end_at::DATE, 'INFINITY')
        JOIN
          user_agent_type
        ON
          user_agent_type.name = visits_fact_stage.user_agent_type_name AND
          user_agent_type.version = visits_fact_stage.user_agent_type_version
        ;
      EOS
    end
  end
end
