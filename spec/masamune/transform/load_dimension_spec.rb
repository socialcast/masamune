require 'spec_helper'
require 'active_support/core_ext/string/strip'

describe Masamune::Transform::LoadDimension do
  let(:mock_data) { StringIO.new }

  let(:user_file) do
    Masamune::Schema::File.new name: 'user',
      columns: [
        Masamune::Schema::Column.new(name: 'id', type: :integer),
        Masamune::Schema::Column.new(name: 'tenant_id', type: :integer),
        Masamune::Schema::Column.new(name: 'department_id', type: :integer),
        Masamune::Schema::Column.new(name: 'admin', type: :boolean),
        Masamune::Schema::Column.new(name: 'preferences', type: :yaml),
        Masamune::Schema::Column.new(name: 'updated_at', type: :timestamp),
        Masamune::Schema::Column.new(name: 'deleted_at', type: :timestamp)
      ]
  end

  let(:user_account_state_type) do
    Masamune::Schema::Dimension.new name: 'user_account_state', type: :mini,
      columns: [
        Masamune::Schema::Column.new(name: 'name', type: :string, unique: true),
        Masamune::Schema::Column.new(name: 'description', type: :string)
      ], rows: [
        Masamune::Schema::Row.new(values: {
          name: 'registered',
          description: 'Registered'
        }),
        Masamune::Schema::Row.new(values: {
          name: 'active',
          description: 'Active',
        }, default: true),
        Masamune::Schema::Row.new(values: {
          name: 'inactive',
          description: 'Inactive'
        })
      ]
  end

  let(:department_dimension) do
    Masamune::Schema::Dimension.new name: 'department', type: :mini, insert: true,
      columns: [
        Masamune::Schema::Column.new(name: 'uuid', type: :uuid, primary_key: true),
        Masamune::Schema::Column.new(name: 'tenant_id', type: :integer, unique: true, surrogate_key: true),
        Masamune::Schema::Column.new(name: 'department_id', type: :integer, unique: true, surrogate_key: true)
      ]
  end

  let(:user_dimension) do
    Masamune::Schema::Dimension.new name: 'user', ledger: true,
      references: [department_dimension, user_account_state_type],
      columns: [
        Masamune::Schema::Column.new(name: 'tenant_id', index: true, surrogate_key: true),
        Masamune::Schema::Column.new(name: 'user_id', index: true, surrogate_key: true),
        Masamune::Schema::Column.new(name: 'preferences', type: :key_value, null: true)
      ]
  end

  let(:map) do
    Masamune::Schema::Map.new(
      fields: {
        'tenant_id'                => 'tenant_id',
        'user_id'                  => 'id',
        'department.department_id' => 'department_id',
        'user_account_state.name'  => ->(row) { row[:deleted_at] ? 'deleted' : 'active' },
        'preferences_now'          => 'preferences',
        'start_at'                 => 'updated_at',
        'source_kind'              => 'groups',
        'delta'                    => 1})
  end

  let(:transform) { described_class.new mock_data, user_file, user_dimension, map }

  before do
    transform.run
  end

  describe '#stage_dimension_as_psql' do
    before do
      allow_any_instance_of(Masamune::Schema::File).to receive(:path) { 'output.csv' }
    end

    subject(:result) { transform.stage_dimension_as_psql }

    it 'should eq render load_dimension template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TEMPORARY TABLE IF NOT EXISTS user_file_stage
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

        COPY user_file_stage FROM 'output.csv' WITH (FORMAT 'csv');
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
          user_file_stage
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
        CREATE TEMPORARY TABLE IF NOT EXISTS department_stage (LIKE department_type INCLUDING ALL);

        INSERT INTO department_stage(tenant_id, department_id)
        SELECT DISTINCT
          tenant_id, department_type_department_id
        FROM
          user_file_stage
        WHERE
          tenant_id IS NOT NULL AND department_type_department_id IS NOT NULL
        ;

        BEGIN;
        LOCK TABLE department_type IN EXCLUSIVE MODE;

        INSERT INTO
          department_type (tenant_id,department_id)
        SELECT
          department_stage.tenant_id,
          department_stage.department_id
        FROM
          department_stage
        LEFT OUTER JOIN
          department_type
        ON
          department_type.tenant_id = department_stage.tenant_id AND
          department_type.department_id = department_stage.department_id
        WHERE
          department_type.tenant_id IS NULL AND
          department_type.department_id IS NULL
        ;

        COMMIT;
      EOS
    end
  end
end
