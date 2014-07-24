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
          values: [
            {
              name: 'active',
              description: 'Active',
              missing_column: true
            }
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
          values: [
            {
              name: 'registered',
              description: 'Registered'
            },
            {
              name: 'active'
            }
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
          WHERE NOT EXISTS (SELECT 1 FROM user_account_state_type WHERE name = 'registered');

          INSERT INTO user_account_state_type (name)
          SELECT 'active'
          WHERE NOT EXISTS (SELECT 1 FROM user_account_state_type WHERE name = 'active');
        EOS
      end
    end

    context 'with default_record values' do
      let(:dimension) do
        described_class.new name: 'user_account_state', type: :mini,
          columns: [
            Masamune::Schema::Column.new(name: 'name', type: :string, unique: true),
            Masamune::Schema::Column.new(name: 'description', type: :string)
          ],
          values: [
            {
              name: 'active',
              default_record: true
            }
          ]
      end

      it 'should eq dimension template' do
        is_expected.to eq <<-EOS.strip_heredoc
          CREATE TABLE IF NOT EXISTS user_account_state_type
          (
            id SERIAL PRIMARY KEY,
            name VARCHAR NOT NULL,
            description VARCHAR NOT NULL,
            default_record BOOLEAN DEFAULT FALSE,
            UNIQUE(name)
          );

          INSERT INTO user_account_state_type (name, default_record)
          SELECT 'active', TRUE
          WHERE NOT EXISTS (SELECT 1 FROM user_account_state_type WHERE name = 'active');
        EOS
      end
    end

    context 'with referenced dimensions' do
      let(:mini_dimension) do
        described_class.new name: 'user_account_state',
          type: :mini,
          columns: [
            Masamune::Schema::Column.new(name: 'id', type: :integer, primary_key: true),
            Masamune::Schema::Column.new(name: 'name', type: :string, unique: true),
            Masamune::Schema::Column.new(name: 'description', type: :string),
            Masamune::Schema::Column.new(name: 'default_record', type: :boolean, default: false)
          ],
          values: [
            {
              name: 'registered',
              description: 'Registered'
            },
            {
              name: 'active',
              description: 'Active',
              default_record: true
            },
            {
              name: 'inactive',
              description: 'Inactive'
            }
          ]
      end

      let(:dimension) do
        described_class.new name: 'user', references: [mini_dimension]
      end

      it 'should eq dimension template' do
        is_expected.to eq <<-EOS.strip_heredoc
          CREATE TABLE IF NOT EXISTS user_account_state_type
          (
            id SERIAL PRIMARY KEY,
            name VARCHAR NOT NULL,
            description VARCHAR NOT NULL,
            default_record BOOLEAN DEFAULT FALSE,
            UNIQUE(name)
          );

          INSERT INTO user_account_state_type (name, description)
          SELECT 'registered', 'Registered'
          WHERE NOT EXISTS (SELECT 1 FROM user_account_state_type WHERE name = 'registered');

          INSERT INTO user_account_state_type (name, description, default_record)
          SELECT 'active', 'Active', TRUE
          WHERE NOT EXISTS (SELECT 1 FROM user_account_state_type WHERE name = 'active');

          INSERT INTO user_account_state_type (name, description)
          SELECT 'inactive', 'Inactive'
          WHERE NOT EXISTS (SELECT 1 FROM user_account_state_type WHERE name = 'inactive');

          CREATE OR REPLACE FUNCTION default_user_account_state_type_id()
          RETURNS INTEGER IMMUTABLE AS $$
            SELECT id FROM user_account_state_type WHERE default_record = TRUE;
          $$ LANGUAGE SQL;

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
  end
end
