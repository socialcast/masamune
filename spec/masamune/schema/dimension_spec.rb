require 'spec_helper'
require 'active_support/core_ext/string/strip'

describe Masamune::Schema::Dimension do
  describe '#to_s' do
    subject { dimension.to_s }

    # TODO add index
    context 'with simple columns' do
      let(:dimension) do
        described_class.new name: 'user',
          columns: [
            Masamune::Schema::Column.new(name: 'tenant_id'),
            Masamune::Schema::Column.new(name: 'user_id')
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

          CREATE INDEX user_dimension_start_at_index ON user_dimension (start_at);
          CREATE INDEX user_dimension_end_at_index ON user_dimension (end_at);
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
            Masamune::Schema::Column.new(name: 'description', type: :string)
          ],
          values: [
            {
              name: 'registered',
              description: 'Registered'
            },
            {
              name: 'active',
              description: 'Active'
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
            UNIQUE(name)
          );

          INSERT INTO user_account_state_type (name, description)
          SELECT 'registered', 'Registered'
          WHERE NOT EXISTS (SELECT 1 FROM user_account_state_type WHERE name = 'registered');

          INSERT INTO user_account_state_type (name, description)
          SELECT 'active', 'Active'
          WHERE NOT EXISTS (SELECT 1 FROM user_account_state_type WHERE name = 'active');

          INSERT INTO user_account_state_type (name, description)
          SELECT 'inactive', 'Inactive'
          WHERE NOT EXISTS (SELECT 1 FROM user_account_state_type WHERE name = 'inactive');

          CREATE TABLE IF NOT EXISTS user_dimension
          (
            uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            user_account_state_type_id INTEGER NOT NULL REFERENCES user_account_state_type(id),
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
