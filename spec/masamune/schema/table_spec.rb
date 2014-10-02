require 'spec_helper'
require 'active_support/core_ext/string/strip'

describe Masamune::Schema::Table do
  subject { table.as_psql }

  context 'with columns' do
    let(:table) do
      described_class.new id: 'user',
        columns: [
          Masamune::Schema::Column.new(id: 'tenant_id'),
          Masamune::Schema::Column.new(id: 'user_id')
        ]
    end

    it 'should eq table template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TABLE IF NOT EXISTS user_table
        (
          uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
          tenant_id INTEGER NOT NULL,
          user_id INTEGER NOT NULL
        );
      EOS
    end
  end

  context 'with index columns' do
    let(:table) do
      described_class.new id: 'user',
        columns: [
          Masamune::Schema::Column.new(id: 'tenant_id', index: true),
          Masamune::Schema::Column.new(id: 'user_id', index: true)
        ]
    end

    it 'should eq table template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TABLE IF NOT EXISTS user_table
        (
          uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
          tenant_id INTEGER NOT NULL,
          user_id INTEGER NOT NULL
        );

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_table_tenant_id_index') THEN
        CREATE INDEX user_table_tenant_id_index ON user_table (tenant_id);
        END IF; END $$;

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_table_user_id_index') THEN
        CREATE INDEX user_table_user_id_index ON user_table (user_id);
        END IF; END $$;
      EOS
    end
  end

  context 'with multiple index columns' do
    let(:table) do
      described_class.new id: 'user',
        columns: [
          Masamune::Schema::Column.new(id: 'tenant_id', index: ['tenant_id', 'shared']),
          Masamune::Schema::Column.new(id: 'user_id', index: ['user_id', 'shared'])
        ]
    end

    it 'should eq table template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TABLE IF NOT EXISTS user_table
        (
          uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
          tenant_id INTEGER NOT NULL,
          user_id INTEGER NOT NULL
        );

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_table_tenant_id_index') THEN
        CREATE INDEX user_table_tenant_id_index ON user_table (tenant_id);
        END IF; END $$;

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_table_user_id_index') THEN
        CREATE INDEX user_table_user_id_index ON user_table (user_id);
        END IF; END $$;

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_table_tenant_id_user_id_index') THEN
        CREATE INDEX user_table_tenant_id_user_id_index ON user_table (tenant_id, user_id);
        END IF; END $$;
      EOS
    end
  end

  context 'with multiple unique columns' do
    let(:table) do
      described_class.new id: 'user',
        columns: [
          Masamune::Schema::Column.new(id: 'tenant_id', unique: ['shared']),
          Masamune::Schema::Column.new(id: 'user_id', unique: ['user_id', 'shared'])
        ]
    end

    it 'should eq table template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TABLE IF NOT EXISTS user_table
        (
          uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
          tenant_id INTEGER NOT NULL,
          user_id INTEGER NOT NULL
        );

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_table_user_id_key') THEN
        ALTER TABLE user_table ADD CONSTRAINT user_table_user_id_key UNIQUE(user_id);
        END IF; END $$;

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_table_tenant_id_user_id_key') THEN
        ALTER TABLE user_table ADD CONSTRAINT user_table_tenant_id_user_id_key UNIQUE(tenant_id, user_id);
        END IF; END $$;
      EOS
    end
  end

  context 'with primary_key columns override' do
    let(:table) do
      described_class.new id: 'user',
        columns: [
          Masamune::Schema::Column.new(id: 'identifier', type: :uuid, primary_key: true),
          Masamune::Schema::Column.new(id: 'name', type: :string)
        ]
    end

    it 'should eq table template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TABLE IF NOT EXISTS user_table
        (
          identifier UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
          name VARCHAR NOT NULL
        );
      EOS
    end
  end

  context 'with invalid values' do
    let(:table) do
      described_class.new id: 'user',
        columns: [
          Masamune::Schema::Column.new(id: 'name', type: :string, unique: true),
          Masamune::Schema::Column.new(id: 'description', type: :string)
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
    let(:table) do
      described_class.new id: 'user',
        columns: [
          Masamune::Schema::Column.new(id: 'name', type: :string),
          Masamune::Schema::Column.new(id: 'description', type: :string)
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

    it 'should eq table template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TABLE IF NOT EXISTS user_table
        (
          uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
          name VARCHAR NOT NULL,
          description VARCHAR NOT NULL
        );

        INSERT INTO user_table (name, description)
        SELECT 'registered', 'Registered'
        WHERE NOT EXISTS (SELECT 1 FROM user_table WHERE name = 'registered' AND description = 'Registered');

        INSERT INTO user_table (name)
        SELECT 'active'
        WHERE NOT EXISTS (SELECT 1 FROM user_table WHERE name = 'active');
      EOS
    end
  end

  context 'with shared unique index' do
    let(:table) do
      described_class.new id: 'user',
        columns: [
          Masamune::Schema::Column.new(id: 'tenant_id', type: :integer, unique: 'tenant_and_user', index: 'tenant_and_user'),
          Masamune::Schema::Column.new(id: 'user_id', type: :integer, unique: 'tenant_and_user', index: 'tenant_and_user')
        ]
    end

    it 'should eq table template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TABLE IF NOT EXISTS user_table
        (
          uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
          tenant_id INTEGER NOT NULL,
          user_id INTEGER NOT NULL
        );

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_table_tenant_id_user_id_key') THEN
        ALTER TABLE user_table ADD CONSTRAINT user_table_tenant_id_user_id_key UNIQUE(tenant_id, user_id);
        END IF; END $$;

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_table_tenant_id_user_id_index') THEN
        CREATE UNIQUE INDEX user_table_tenant_id_user_id_index ON user_table (tenant_id, user_id);
        END IF; END $$;
      EOS
    end
  end

  context 'with multiple default and named rows' do
    let(:table) do
      described_class.new id: 'user',
        columns: [
          Masamune::Schema::Column.new(id: 'uuid', type: :uuid, primary_key: true),
          Masamune::Schema::Column.new(id: 'tenant_id', type: :integer, surrogate_key: true),
          Masamune::Schema::Column.new(id: 'user_id', type: :integer, surrogate_key: true)
        ],
        rows: [
          Masamune::Schema::Row.new(values: {
            tenant_id: 'default_tenant_id()',
            user_id: -1,
          }, default: true),
          Masamune::Schema::Row.new(values: {
            tenant_id: 'default_tenant_id()',
            user_id: -2,
          }, id: 'unknown')
        ]
    end

    it 'should eq table template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TABLE IF NOT EXISTS user_table
        (
          uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
          tenant_id INTEGER NOT NULL,
          user_id INTEGER NOT NULL
        );

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_table_tenant_id_user_id_key') THEN
        ALTER TABLE user_table ADD CONSTRAINT user_table_tenant_id_user_id_key UNIQUE(tenant_id, user_id);
        END IF; END $$;

        INSERT INTO user_table (tenant_id, user_id)
        SELECT default_tenant_id(), -1
        WHERE NOT EXISTS (SELECT 1 FROM user_table WHERE tenant_id = default_tenant_id() AND user_id = -1);

        INSERT INTO user_table (tenant_id, user_id)
        SELECT default_tenant_id(), -2
        WHERE NOT EXISTS (SELECT 1 FROM user_table WHERE tenant_id = default_tenant_id() AND user_id = -2);

        CREATE OR REPLACE FUNCTION default_user_id()
        RETURNS INTEGER IMMUTABLE AS $$
          SELECT -1;
        $$ LANGUAGE SQL;

        CREATE OR REPLACE FUNCTION default_user_table_uuid()
        RETURNS UUID IMMUTABLE AS $$
          SELECT uuid FROM user_table WHERE tenant_id = default_tenant_id() AND user_id = -1;
        $$ LANGUAGE SQL;

        CREATE OR REPLACE FUNCTION unknown_user_id()
        RETURNS INTEGER IMMUTABLE AS $$
          SELECT -2;
        $$ LANGUAGE SQL;

        CREATE OR REPLACE FUNCTION unknown_user_table_uuid()
        RETURNS UUID IMMUTABLE AS $$
          SELECT uuid FROM user_table WHERE tenant_id = default_tenant_id() AND user_id = -2;
        $$ LANGUAGE SQL;
      EOS
    end
  end

  context 'with referenced tables' do
    let(:mini_table) do
      described_class.new id: 'user_account_state',
        columns: [
          Masamune::Schema::Column.new(id: 'name', type: :string, unique: true),
          Masamune::Schema::Column.new(id: 'description', type: :string)
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

    let(:table) do
      described_class.new id: 'user', references: [mini_table],
        columns: [
          Masamune::Schema::Column.new(id: 'name', type: :string)
        ]
    end

    it 'should eq table template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TABLE IF NOT EXISTS user_table
        (
          uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
          user_account_state_table_uuid UUID NOT NULL REFERENCES user_account_state_table(uuid) DEFAULT default_user_account_state_table_uuid(),
          name VARCHAR NOT NULL
        );

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_table_user_account_state_table_uuid_index') THEN
        CREATE INDEX user_table_user_account_state_table_uuid_index ON user_table (user_account_state_table_uuid);
        END IF; END $$;
      EOS
    end

    describe '#as_file' do
      let(:columns) { ['user_account_state.name', 'name'] }

      subject(:file) { table.as_file(columns) }

      it { expect(file.columns).to include :user_account_state_table_name }
      it { expect(file.columns).to include :name }

      it 'should reference mini_table' do
        expect(file.columns[:user_account_state_table_name].reference).to eq(mini_table)
      end
    end
  end

  context 'with labeled referenced table' do
    let(:mini_table) do
      described_class.new id: 'user_account_state', label: 'actor',
        columns: [
          Masamune::Schema::Column.new(id: 'name', type: :string, unique: true),
          Masamune::Schema::Column.new(id: 'description', type: :string)
        ]
    end

    let(:table) do
      described_class.new id: 'user', references: [mini_table],
        columns: [
          Masamune::Schema::Column.new(id: 'name', type: :string)
        ]
    end

    it 'should eq table template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TABLE IF NOT EXISTS user_table
        (
          uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
          actor_user_account_state_table_uuid UUID NOT NULL REFERENCES user_account_state_table(uuid),
          name VARCHAR NOT NULL
        );

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_table_actor_user_account_state_table_uuid_index') THEN
        CREATE INDEX user_table_actor_user_account_state_table_uuid_index ON user_table (actor_user_account_state_table_uuid);
        END IF; END $$;
      EOS
    end
  end

  context 'stage table' do
    let(:mini_table) do
      described_class.new id: 'user_account_state',
        columns: [
          Masamune::Schema::Column.new(id: 'name', type: :string, unique: true),
          Masamune::Schema::Column.new(id: 'description', type: :string)
        ]
    end

    let(:table) do
      described_class.new id: 'user', references: [mini_table],
        columns: [
          Masamune::Schema::Column.new(id: 'name', type: :string)
        ]
    end

    let!(:stage_table) { table.stage_table }

    it 'should duplicate columns' do
      expect(table.parent).to be_nil
      expect(table.columns[:name].parent).to eq(table)
      expect(stage_table.parent).to eq(table)
      expect(stage_table.columns[:name].parent).to eq(stage_table)
    end

    describe '#as_psql' do
      subject { stage_table.as_psql }

      it 'should eq table template' do
        is_expected.to eq <<-EOS.strip_heredoc
          CREATE TEMPORARY TABLE IF NOT EXISTS user_table_stage
          (
            uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            user_account_state_table_uuid UUID,
            name VARCHAR
          );

          CREATE INDEX user_table_stage_user_account_state_table_uuid_index ON user_table_stage (user_account_state_table_uuid);
        EOS
      end
    end
  end
end
