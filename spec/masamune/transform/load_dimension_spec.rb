require 'spec_helper'
require 'active_support/core_ext/string/strip'

describe Masamune::Transform::LoadDimension do
  describe '#as_psql' do
    let(:mini_dimension) do
      Masamune::Schema::Dimension.new name: 'user_account_state', type: :mini,
        columns: [
          Masamune::Schema::Column.new(name: 'name', type: :string, unique: true),
          Masamune::Schema::Column.new(name: 'description', type: :string)
        ]
    end

    let(:file) do
      Masamune::Schema::File.new name: 'user',
        columns: [
          Masamune::Schema::Column.new(name: 'id', type: :integer),
          Masamune::Schema::Column.new(name: 'tenant_id', type: :integer),
          Masamune::Schema::Column.new(name: 'admin', type: :boolean),
          Masamune::Schema::Column.new(name: 'updated_at', type: :timestamp),
          Masamune::Schema::Column.new(name: 'deleted_at', type: :timestamp)
        ]
    end

    let(:dimension) do
      Masamune::Schema::Dimension.new name: 'user', ledger: true,
        references: [mini_dimension],
        columns: [
          Masamune::Schema::Column.new(name: 'tenant_id', index: true, surrogate_key: true),
          Masamune::Schema::Column.new(name: 'user_id', index: true, surrogate_key: true)
        ]
    end

    let(:map) do
      Masamune::Schema::Map.new(
        fields: {
          'tenant_id'               => 'tenant_id',
          'user_id'                 => 'id',
          'user_account_state.name' => ->(row) { row[:deleted_at] ? 'deleted' : 'active' },
          'start_at'                => 'updated_at',
          'source_kind'             => 'groups',
          'delta'                   => 1})
    end

    context 'with file and ledger dimension' do
      before do
        allow_any_instance_of(Masamune::Schema::File).to receive(:path) { 'output.csv' }
      end

      let(:transform) { described_class.new file, dimension, map }

      subject(:result) { transform.as_psql }

      it 'should eq render load_dimension template' do
        is_expected.to eq <<-EOS.strip_heredoc
          CREATE TEMPORARY TABLE IF NOT EXISTS user_file_stage
          (
            tenant_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            user_account_state_type_name VARCHAR NOT NULL,
            start_at TIMESTAMP NOT NULL,
            source_kind VARCHAR,
            delta INTEGER NOT NULL
          );

          COPY user_file_stage FROM 'output.csv' WITH (FORMAT 'csv');

          CREATE TEMPORARY TABLE IF NOT EXISTS user_dimension_ledger_stage (LIKE user_dimension_ledger INCLUDING ALL);

          INSERT INTO
            user_dimension_ledger_stage (tenant_id, user_id, user_account_state_type_id, start_at, source_kind, delta)
          SELECT
            tenant_id,
            user_id,
            (SELECT id FROM user_account_state_type WHERE user_account_state_type.name = user_account_state_type_name),
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
            user_account_state_type_id = user_dimension_ledger_stage.user_account_state_type_id
          FROM
            user_dimension_ledger_stage
          WHERE
            user_dimension_ledger.tenant_id = user_dimension_ledger_stage.tenant_id AND
            user_dimension_ledger.user_id = user_dimension_ledger_stage.user_id AND
            user_dimension_ledger.source_kind = user_dimension_ledger_stage.source_kind AND
            user_dimension_ledger.source_uuid = user_dimension_ledger_stage.source_uuid AND
            user_dimension_ledger.start_at = user_dimension_ledger_stage.start_at
          ;

          INSERT INTO
            user_dimension_ledger (user_account_state_type_id,tenant_id,user_id,source_kind,source_uuid,start_at,last_modified_at,delta)
          SELECT
            user_dimension_ledger_stage.user_account_state_type_id,
            user_dimension_ledger_stage.tenant_id,
            user_dimension_ledger_stage.user_id,
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
            user_dimension_ledger.source_uuid = user_dimension_ledger_stage.source_uuid AND
            user_dimension_ledger.start_at = user_dimension_ledger_stage.start_at
          WHERE
            user_dimension_ledger.tenant_id IS NULL AND
            user_dimension_ledger.user_id IS NULL AND
            user_dimension_ledger.source_kind IS NULL AND
            user_dimension_ledger.source_uuid IS NULL AND
            user_dimension_ledger.start_at IS NULL
          ;

          COMMIT;
        EOS
      end
    end
  end
end
