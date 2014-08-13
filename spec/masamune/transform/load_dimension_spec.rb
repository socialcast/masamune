require 'spec_helper'
require 'active_support/core_ext/string/strip'

describe Masamune::Transform::LoadDimension do
  let(:environment) { double }
  let(:registry) { Masamune::Schema::Registry.new(environment) }

  before do
    registry.schema do
      dimension 'cluster', type: :mini do
        column 'id', type: :integer, primary_key: true, auto: true
        column 'name', type: :string, unique: true
        row name: 'default', attributes: {default: true}
      end

      dimension 'user_account_state', type: :mini do
        column 'name', type: :string, unique: true
        column 'description', type: :string
        row name: 'registered', description: 'Registered'
        row name: 'active', description: 'Active', attributes: {default: true}
        row name: 'inactive', description: 'Inactive'
      end

      dimension 'department', type: :mini, insert: true do
        references :cluster
        column 'uuid', type: :uuid, primary_key: true
        column 'tenant_id', type: :integer, unique: true, surrogate_key: true
        column 'department_id', type: :integer, unique: true, surrogate_key: true
      end

      dimension 'user', type: :four do
        references :cluster
        references :department
        references :user_account_state
        column 'tenant_id', index: true, surrogate_key: true
        column 'user_id', index: true, surrogate_key: true
        column 'preferences', type: :key_value, null: true
      end

      file 'user' do
        column  'tenant_id', type: :integer
        column  'user_id', type: :integer
        column  'department.department_id', type: :integer
        column  'user_account_state.name', type: :string
        column  'preferences_now', type: :json
        column  'start_at', type: :timestamp
        column  'source_kind', type: :string
        column  'delta', type: :integer
      end
    end
  end

  let(:data) { double(path: 'output.csv') }
  let(:target) { registry.dimensions[:user].ledger_table }
  let(:source) { registry.files[:user].as_table(target) }

  let(:transform) { described_class.new data, source, target }

  describe '#stage_dimension_as_psql' do
    subject(:result) { transform.stage_dimension_as_psql }

    it 'should eq render load_dimension template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TEMPORARY TABLE IF NOT EXISTS user_dimension_ledger_stage
        (
          tenant_id INTEGER,
          user_id INTEGER,
          department_type_department_id INTEGER,
          user_account_state_type_name VARCHAR,
          preferences_now JSON,
          start_at TIMESTAMP,
          source_kind VARCHAR,
          delta INTEGER
        );

        COPY user_dimension_ledger_stage FROM 'output.csv' WITH (FORMAT 'csv');

        CREATE INDEX user_dimension_ledger_stage_tenant_id_index ON user_dimension_ledger_stage (tenant_id);
        CREATE INDEX user_dimension_ledger_stage_user_id_index ON user_dimension_ledger_stage (user_id);
        CREATE INDEX user_dimension_ledger_stage_start_at_index ON user_dimension_ledger_stage (start_at);
      EOS
    end
  end

  describe '#load_dimension_as_psql' do
    subject(:result) { transform.load_dimension_as_psql }

    it 'should eq render load_dimension template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TEMPORARY TABLE IF NOT EXISTS user_dimension_ledger_stage (LIKE user_dimension_ledger INCLUDING ALL);

        INSERT INTO
          user_dimension_ledger_stage (tenant_id, user_id, department_type_uuid, user_account_state_type_id, preferences_now, start_at, source_kind, delta)
        SELECT
          tenant_id,
          user_id,
          (SELECT uuid FROM department_type WHERE department_type.department_id = department_type_department_id),
          COALESCE((SELECT id FROM user_account_state_type WHERE user_account_state_type.name = user_account_state_type_name), default_user_account_state_type_id()),
          json_to_hstore(preferences_now),
          start_at,
          source_kind,
          delta
        FROM
          user_dimension_ledger_stage
        ;

        BEGIN;
        LOCK TABLE user_dimension_ledger IN EXCLUSIVE MODE;

        UPDATE
          user_dimension_ledger
        SET
          department_type_uuid = user_dimension_ledger_stage.department_type_uuid,
          user_account_state_type_id = user_dimension_ledger_stage.user_account_state_type_id,
          preferences_now = user_dimension_ledger_stage.preferences_now,
          preferences_was = user_dimension_ledger_stage.preferences_was
        FROM
          user_dimension_ledger_stage
        WHERE
          user_dimension_ledger.tenant_id = user_dimension_ledger_stage.tenant_id AND
          user_dimension_ledger.user_id = user_dimension_ledger_stage.user_id AND
          user_dimension_ledger.source_kind = user_dimension_ledger_stage.source_kind AND
          user_dimension_ledger.start_at = user_dimension_ledger_stage.start_at
        ;

        INSERT INTO
          user_dimension_ledger (department_type_uuid,user_account_state_type_id,tenant_id,user_id,preferences_now,preferences_was,source_kind,source_uuid,start_at,last_modified_at,delta)
        SELECT
          user_dimension_ledger_stage.department_type_uuid,
          user_dimension_ledger_stage.user_account_state_type_id,
          user_dimension_ledger_stage.tenant_id,
          user_dimension_ledger_stage.user_id,
          user_dimension_ledger_stage.preferences_now,
          user_dimension_ledger_stage.preferences_was,
          user_dimension_ledger_stage.source_kind,
          user_dimension_ledger_stage.source_uuid,
          user_dimension_ledger_stage.start_at,
          user_dimension_ledger_stage.last_modified_at,
          user_dimension_ledger_stage.delta
        FROM
          user_dimension_ledger_stage
        LEFT OUTER JOIN
          user_dimension_ledger
        ON
          user_dimension_ledger.tenant_id = user_dimension_ledger_stage.tenant_id AND
          user_dimension_ledger.user_id = user_dimension_ledger_stage.user_id AND
          user_dimension_ledger.source_kind = user_dimension_ledger_stage.source_kind AND
          user_dimension_ledger.start_at = user_dimension_ledger_stage.start_at
        WHERE
          user_dimension_ledger.tenant_id IS NULL AND
          user_dimension_ledger.user_id IS NULL AND
          user_dimension_ledger.source_kind IS NULL AND
          user_dimension_ledger.start_at IS NULL
        ;

        COMMIT;
      EOS
    end
  end

  describe '#insert_reference_values_as_psql' do
    subject(:result) { transform.insert_reference_values_as_psql }

    it 'should eq render template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TEMPORARY TABLE IF NOT EXISTS department_type_stage (LIKE department_type INCLUDING ALL);

        INSERT INTO department_type_stage(tenant_id, department_id)
        SELECT DISTINCT
          tenant_id, department_type_department_id
        FROM
          user_dimension_ledger_stage
        WHERE
          tenant_id IS NOT NULL AND department_type_department_id IS NOT NULL
        ;

        BEGIN;
        LOCK TABLE department_type IN EXCLUSIVE MODE;

        INSERT INTO
          department_type (tenant_id,department_id)
        SELECT
          department_type_stage.tenant_id,
          department_type_stage.department_id
        FROM
          department_type_stage
        LEFT OUTER JOIN
          department_type
        ON
          department_type.tenant_id = department_type_stage.tenant_id AND
          department_type.department_id = department_type_stage.department_id
        WHERE
          department_type.tenant_id IS NULL AND
          department_type.department_id IS NULL
        ;

        COMMIT;
      EOS
    end
  end
end
