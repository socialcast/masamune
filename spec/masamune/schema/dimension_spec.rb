require 'spec_helper'
require 'active_support/core_ext/string/strip'

describe Masamune::Schema::Dimension do
  describe '#to_s' do
    subject { dimension.to_s }

    context 'with simple columns' do
      let(:dimension) do
        described_class.new name: 'user',
          columns: [
            Masamune::Schema::Column.new(name: 'tenant_id', index: true),
            Masamune::Schema::Column.new(name: 'user_id', index: true)
          ]
      end

      it 'should eq dimension template' do
        is_expected.to eq <<-EOS.strip_heredoc
          CREATE TABLE IF NOT EXISTS user_dimension
          (
            uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            tenant_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            start_at TIMESTAMP DEFAULT TO_TIMESTAMP(0),
            end_at TIMESTAMP,
            version INTEGER DEFAULT 1,
            last_modified_at TIMESTAMP DEFAULT NOW()
          );

          CREATE INDEX user_dimension_tenant_id_index ON user_dimension (tenant_id);
          CREATE INDEX user_dimension_user_id_index ON user_dimension (user_id);
          CREATE INDEX user_dimension_start_at_index ON user_dimension (start_at);
          CREATE INDEX user_dimension_end_at_index ON user_dimension (end_at);
        EOS
      end
    end

    context 'with primary_key columns override' do
      let(:dimension) do
        described_class.new name: 'user_account_state', type: :mini,
          columns: [
            Masamune::Schema::Column.new(name: 'uuid', type: :uuid, primary_key: true),
            Masamune::Schema::Column.new(name: 'name', type: :string)
          ]
      end

      it 'should eq dimension template' do
        is_expected.to eq <<-EOS.strip_heredoc
          CREATE TABLE IF NOT EXISTS user_account_state_type
          (
            uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            name VARCHAR NOT NULL
          );
        EOS
      end
    end

    context 'with invalid values' do
      let(:dimension) do
        described_class.new name: 'user_account_state', type: :mini,
          columns: [
            Masamune::Schema::Column.new(name: 'name', type: :string, unique: true),
            Masamune::Schema::Column.new(name: 'description', type: :string)
          ],
          rows: [
            Masamune::Schema::Row.new(values: {
              name: 'active',
              description: 'Active',
              missing_column: true
            })
          ]
      end

      it { expect { subject }.to raise_error ArgumentError, /contains undefined columns/ }
    end

    context 'with partial values' do
      let(:dimension) do
        described_class.new name: 'user_account_state', type: :mini,
          columns: [
            Masamune::Schema::Column.new(name: 'name', type: :string, unique: true),
            Masamune::Schema::Column.new(name: 'description', type: :string)
          ],
          rows: [
            Masamune::Schema::Row.new(values: {
              name: 'registered',
              description: 'Registered'
            }),
            Masamune::Schema::Row.new(values: {
              name: 'active'
            })
          ]
      end

      it 'should eq dimension template' do
        is_expected.to eq <<-EOS.strip_heredoc
          CREATE TABLE IF NOT EXISTS user_account_state_type
          (
            id SERIAL PRIMARY KEY,
            name VARCHAR NOT NULL,
            description VARCHAR NOT NULL,
            UNIQUE(name)
          );

          INSERT INTO user_account_state_type (name, description)
          SELECT 'registered', 'Registered'
          WHERE NOT EXISTS (SELECT 1 FROM user_account_state_type WHERE name = 'registered' AND description = 'Registered');

          INSERT INTO user_account_state_type (name)
          SELECT 'active'
          WHERE NOT EXISTS (SELECT 1 FROM user_account_state_type WHERE name = 'active');
        EOS
      end
    end

    context 'with shared index' do
      let(:dimension) do
        described_class.new name: 'user', type: :mini,
          columns: [
            Masamune::Schema::Column.new(name: 'tenant_id', type: :integer, unique: true, index: 'tenant_and_user'),
            Masamune::Schema::Column.new(name: 'user_id', type: :integer, unique: true, index: 'tenant_and_user')
          ]
      end

      it 'should eq dimension template' do
        is_expected.to eq <<-EOS.strip_heredoc
          CREATE TABLE IF NOT EXISTS user_type
          (
            id SERIAL PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            UNIQUE(tenant_id, user_id)
          );

          CREATE UNIQUE INDEX user_type_tenant_id_user_id_index ON user_type (tenant_id, user_id);
        EOS
      end
    end

    context 'with multiple default and named rows' do
      let(:dimension) do
        described_class.new name: 'user', type: :mini,
          columns: [
            Masamune::Schema::Column.new(name: 'uuid', type: :uuid, primary_key: true),
            Masamune::Schema::Column.new(name: 'tenant_id', type: :integer, unique: true),
            Masamune::Schema::Column.new(name: 'user_id', type: :integer, unique: true, surrogate_key: true)
          ],
          rows: [
            Masamune::Schema::Row.new(values: {
              tenant_id: 'default_tenant_id()',
              user_id: -1,
            }, default: true),
            Masamune::Schema::Row.new(values: {
              tenant_id: 'default_tenant_id()',
              user_id: -2,
            }, name: 'unknown')
          ]
      end

      it 'should eq dimension template' do
        is_expected.to eq <<-EOS.strip_heredoc
          CREATE TABLE IF NOT EXISTS user_type
          (
            uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            tenant_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            UNIQUE(tenant_id, user_id)
          );

          INSERT INTO user_type (tenant_id, user_id)
          SELECT default_tenant_id(), -1
          WHERE NOT EXISTS (SELECT 1 FROM user_type WHERE tenant_id = default_tenant_id() AND user_id = -1);

          INSERT INTO user_type (tenant_id, user_id)
          SELECT default_tenant_id(), -2
          WHERE NOT EXISTS (SELECT 1 FROM user_type WHERE tenant_id = default_tenant_id() AND user_id = -2);

          CREATE OR REPLACE FUNCTION default_user_id()
          RETURNS INTEGER IMMUTABLE AS $$
            SELECT -1;
          $$ LANGUAGE SQL;

          CREATE OR REPLACE FUNCTION default_user_type_uuid()
          RETURNS UUID IMMUTABLE AS $$
            SELECT uuid FROM user_type WHERE tenant_id = default_tenant_id() AND user_id = -1;
          $$ LANGUAGE SQL;

          CREATE OR REPLACE FUNCTION unknown_user_id()
          RETURNS INTEGER IMMUTABLE AS $$
            SELECT -2;
          $$ LANGUAGE SQL;

          CREATE OR REPLACE FUNCTION unknown_user_type_uuid()
          RETURNS UUID IMMUTABLE AS $$
            SELECT uuid FROM user_type WHERE tenant_id = default_tenant_id() AND user_id = -2;
          $$ LANGUAGE SQL;
        EOS
      end
    end

    context 'with referenced dimensions' do
      let(:mini_dimension) do
        described_class.new name: 'user_account_state', type: :mini,
          columns: [
            Masamune::Schema::Column.new(name: 'name', type: :string, unique: true),
            Masamune::Schema::Column.new(name: 'description', type: :string)
          ],
          rows: [
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

      let(:dimension) do
        described_class.new name: 'user', references: [mini_dimension]
      end

      it 'should eq dimension template' do
        is_expected.to eq <<-EOS.strip_heredoc
          CREATE TABLE IF NOT EXISTS user_dimension
          (
            uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            user_account_state_type_id INTEGER REFERENCES user_account_state_type(id) DEFAULT default_user_account_state_type_id(),
            start_at TIMESTAMP DEFAULT TO_TIMESTAMP(0),
            end_at TIMESTAMP,
            version INTEGER DEFAULT 1,
            last_modified_at TIMESTAMP DEFAULT NOW()
          );

          CREATE INDEX user_dimension_start_at_index ON user_dimension (start_at);
          CREATE INDEX user_dimension_end_at_index ON user_dimension (end_at);
        EOS
      end
    end

    context 'for type :two dimension with :ledger' do
      let(:dimension) do
        described_class.new name: 'user', type: :two, ledger: true,
          columns: [
            Masamune::Schema::Column.new(name: 'tenant_id', index: true),
            Masamune::Schema::Column.new(name: 'user_id', index: true),
            Masamune::Schema::Column.new(name: 'preferences', type: :key_value, null: true)
          ]
      end

      it 'should eq dimension template' do
        is_expected.to eq <<-EOS.strip_heredoc
          CREATE TABLE IF NOT EXISTS user_dimension_ledger
          (
            uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            tenant_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            preferences_now HSTORE,
            preferences_was HSTORE,
            source_kind VARCHAR,
            source_uuid VARCHAR,
            start_at TIMESTAMP NOT NULL,
            last_modified_at TIMESTAMP DEFAULT NOW(),
            delta INTEGER NOT NULL
          );

          CREATE INDEX user_dimension_ledger_tenant_id_index ON user_dimension_ledger (tenant_id);
          CREATE INDEX user_dimension_ledger_user_id_index ON user_dimension_ledger (user_id);
          CREATE INDEX user_dimension_ledger_start_at_index ON user_dimension_ledger (start_at);

          CREATE TABLE IF NOT EXISTS user_dimension
          (
            uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            tenant_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            preferences HSTORE,
            parent_uuid UUID REFERENCES user_dimension_ledger(uuid),
            record_uuid UUID REFERENCES user_dimension_ledger(uuid),
            start_at TIMESTAMP DEFAULT TO_TIMESTAMP(0),
            end_at TIMESTAMP,
            version INTEGER DEFAULT 1,
            last_modified_at TIMESTAMP DEFAULT NOW()
          );

          CREATE INDEX user_dimension_tenant_id_index ON user_dimension (tenant_id);
          CREATE INDEX user_dimension_user_id_index ON user_dimension (user_id);
          CREATE INDEX user_dimension_start_at_index ON user_dimension (start_at);
          CREATE INDEX user_dimension_end_at_index ON user_dimension (end_at);
        EOS
      end
    end
  end

  describe '#load' do
    let(:environment) { double }

    let(:mini_dimension) do
      described_class.new name: 'user_account_state', type: :mini,
        columns: [
          Masamune::Schema::Column.new(name: 'name', type: :string, unique: true),
          Masamune::Schema::Column.new(name: 'description', type: :string)
        ]
    end

    let(:input) do
      Masamune::Schema::CSVFile.new environment, name: 'user',
        columns: [
          Masamune::Schema::Column.new(name: 'user_id', type: :integer),
          Masamune::Schema::Column.new(name: 'tenant_id', type: :integer),
          Masamune::Schema::Column.new(reference: mini_dimension, name: 'name', type: :string),
          Masamune::Schema::Column.new(name: 'start_at', type: :timestamp)
        ],
        constants: {
          source_kind: 'latest/users',
          delta: 0
        }
    end

    let(:dimension) do
      described_class.new name: 'user', ledger: true,
        references: [mini_dimension],
        columns: [
          Masamune::Schema::Column.new(name: 'tenant_id', index: true, surrogate_key: true),
          Masamune::Schema::Column.new(name: 'user_id', index: true, surrogate_key: true)
        ]
    end

    # FIXME rename user_stage_dimension_ledger to user_dimension_ledger_stage
    context 'with input data' do
      before do
        allow(input).to receive(:transform) { double(path: 'output.csv') }
      end

      subject(:result) { dimension.ledger_table.load(input) }

      it 'should eq render load template' do
        is_expected.to eq <<-EOS.strip_heredoc
          CREATE TEMPORARY TABLE IF NOT EXISTS user_stage
          (
            user_id INTEGER NOT NULL,
            tenant_id INTEGER NOT NULL,
            user_account_state_type_name VARCHAR NOT NULL,
            start_at TIMESTAMP NOT NULL
          );

          COPY user_stage FROM 'output.csv' WITH (FORMAT 'csv');

          CREATE TEMPORARY TABLE IF NOT EXISTS user_stage_dimension_ledger (LIKE user_dimension_ledger INCLUDING ALL);

          INSERT INTO
            user_stage_dimension_ledger (user_id, tenant_id, user_account_state_type_id, start_at, source_kind, delta)
          SELECT
            user_id,
            tenant_id,
            (SELECT id FROM user_account_state_type WHERE user_account_state_type.name = user_account_state_type_name),
            start_at,
            'latest/users',
            0
          FROM
            user_stage
          ;

          BEGIN;
          LOCK TABLE user_dimension_ledger IN EXCLUSIVE MODE;

          UPDATE
            user_dimension_ledger
          SET
            user_account_state_type_id = user_stage_dimension_ledger.user_account_state_type_id
          FROM
            user_stage_dimension_ledger
          WHERE
            user_dimension_ledger.tenant_id = user_stage_dimension_ledger.tenant_id AND
            user_dimension_ledger.user_id = user_stage_dimension_ledger.user_id AND
            user_dimension_ledger.source_kind = user_stage_dimension_ledger.source_kind AND
            user_dimension_ledger.source_uuid = user_stage_dimension_ledger.source_uuid AND
            user_dimension_ledger.start_at = user_stage_dimension_ledger.start_at
          ;

          INSERT INTO
            user_dimension_ledger (user_account_state_type_id,tenant_id,user_id,source_kind,source_uuid,start_at,last_modified_at,delta)
          SELECT
            user_stage_dimension_ledger.user_account_state_type_id,
            user_stage_dimension_ledger.tenant_id,
            user_stage_dimension_ledger.user_id,
            user_stage_dimension_ledger.source_kind,
            user_stage_dimension_ledger.source_uuid,
            user_stage_dimension_ledger.start_at,
            user_stage_dimension_ledger.last_modified_at,
            user_stage_dimension_ledger.delta
          FROM
            user_stage_dimension_ledger
          LEFT OUTER JOIN
            user_dimension_ledger
          ON
            user_dimension_ledger.tenant_id = user_stage_dimension_ledger.tenant_id AND
            user_dimension_ledger.user_id = user_stage_dimension_ledger.user_id AND
            user_dimension_ledger.source_kind = user_stage_dimension_ledger.source_kind AND
            user_dimension_ledger.source_uuid = user_stage_dimension_ledger.source_uuid AND
            user_dimension_ledger.start_at = user_stage_dimension_ledger.start_at
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
