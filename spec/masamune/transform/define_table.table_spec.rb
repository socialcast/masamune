require 'spec_helper'

describe Masamune::Transform::DefineTable do
  subject { transform.define_table(target).to_s }

  context 'for postgres table with columns' do
    before do
      registry.schema :postgres do
        table 'user' do
          column 'tenant_id'
          column 'user_id'
        end
      end
    end

    let(:target) { registry.postgres.user_table }

    it 'should render table template' do
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

  context 'for postgres table with index columns' do
    before do
      registry.schema :postgres do
        table 'user' do
          column 'tenant_id', index: true
          column 'user_id', index: true
        end
      end
    end

    let(:target) { registry.postgres.user_table }

    it 'should render table template' do
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

  context 'for postgres table with multiple index columns' do
    before do
      registry.schema :postgres do
        table 'user' do
          column 'tenant_id', index: ['tenant_id', 'shared']
          column 'user_id', index: ['user_id', 'shared']
        end
      end
    end

    let(:target) { registry.postgres.user_table }

    it 'should render table template' do
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

  context 'for postgres table with multiple unique columns' do
    before do
      registry.schema :postgres do
        table 'user' do
          column 'tenant_id', unique: ['shared']
          column 'user_id', unique: ['user_id', 'shared']
        end
      end
    end

    let(:target) { registry.postgres.user_table }

    it 'should render table template' do
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

  context 'for postgres table with enum column' do
    before do
      registry.schema :postgres do
        table 'user' do
          column 'tenant_id'
          column 'user_id'
          column 'state', type: :enum, sub_type: :user_state, values: %w(active inactive terminated), default: 'active'
        end
      end
    end

    let(:target) { registry.postgres.user_table }

    it 'should render table template' do
      is_expected.to eq <<-EOS.strip_heredoc
        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_type t WHERE LOWER(t.typname) = LOWER('USER_STATE_TYPE')) THEN
        CREATE TYPE USER_STATE_TYPE AS ENUM ('active', 'inactive', 'terminated');
        END IF; END $$;

        CREATE TABLE IF NOT EXISTS user_table
        (
          uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
          tenant_id INTEGER NOT NULL,
          user_id INTEGER NOT NULL,
          state USER_STATE_TYPE NOT NULL DEFAULT 'active'
        );
      EOS
    end

    context '#stage_table' do
      let(:target) { registry.postgres.user_table.stage_table }

      it 'should render table template' do
        is_expected.to eq <<-EOS.strip_heredoc
          CREATE TEMPORARY TABLE IF NOT EXISTS user_table_stage
          (
            uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            tenant_id INTEGER,
            user_id INTEGER,
            state USER_STATE_TYPE DEFAULT 'active'
          );
        EOS
      end
    end
  end

  context 'for postgres table with surrogate_key columns override' do
    before do
      registry.schema :postgres do
        table 'user' do
          column 'identifier', type: :uuid, surrogate_key: true
          column 'name', type: :string
        end
      end
    end

    let(:target) { registry.postgres.user_table }

    it 'should render table template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TABLE IF NOT EXISTS user_table
        (
          identifier UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
          name VARCHAR NOT NULL
        );
      EOS
    end
  end

  context 'for postgres table with partial values' do
    before do
      registry.schema :postgres do
        table 'user' do
          column 'name', type: :string
          column 'description', type: :string
          row name: 'registered', description: 'Registered'
          row name: 'active'
        end
      end
    end

    let(:target) { registry.postgres.user_table }

    it 'should render table template' do
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

  context 'for postgres table with shared unique index' do
    before do
      registry.schema :postgres do
        table 'user' do
          column 'tenant_id', unique: 'tenant_and_user', index: 'tenant_and_user'
          column 'user_id', unique: 'tenant_and_user', index: 'tenant_and_user'
        end
      end
    end

    let(:target) { registry.postgres.user_table }

    it 'should render table template' do
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

  context 'for postgres table with multiple default and named rows' do
    before do
      registry.schema :postgres do
        table 'user' do
          column 'uuid', type: :uuid, surrogate_key: true
          column 'tenant_id', type: :integer, natural_key: true
          column 'user_id', type: :integer, natural_key: true
          row tenant_id: 'default_tenant_id()', user_id: -1, attributes: {default: true}
          row tenant_id: 'default_tenant_id()', user_id: -2, attributes: {id: 'unknown'}
        end
      end
    end

    let(:target) { registry.postgres.user_table }

    it 'should render table template' do
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

  context 'for postgres table with referenced tables' do
    before do
      registry.schema :postgres do
        table 'user_account_state' do
          column 'name', type: :string, unique: true
          column 'description', type: :string
          row name: 'registered', description: 'Registered'
          row name: 'active', description: 'Active', attributes: { default: true }
          row name: 'inactive', description: 'Inactive'
        end

        table 'user' do
          references :user_account_state
          column 'name', type: :string
        end
      end
    end

    let(:target) { registry.postgres.user_table }

    it 'should render table template' do
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
  end

  context 'for postgres table with labeled referenced table' do
    before do
      registry.schema :postgres do
        table 'user_account_state' do
          column 'name', type: :string, unique: true
          column 'description', type: :string
          row name: 'active', description: 'Active', attributes: { default: true }
        end

        table 'user' do
          references :user_account_state
          references :user_account_state, label: 'hr', null: true, default: :null
          column 'name', type: :string
        end
      end
    end

    let(:target) { registry.postgres.user_table }

    it 'should render table template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TABLE IF NOT EXISTS user_table
        (
          uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
          user_account_state_table_uuid UUID NOT NULL REFERENCES user_account_state_table(uuid) DEFAULT default_user_account_state_table_uuid(),
          hr_user_account_state_table_uuid UUID REFERENCES user_account_state_table(uuid),
          name VARCHAR NOT NULL
        );

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_table_user_account_state_table_uuid_index') THEN
        CREATE INDEX user_table_user_account_state_table_uuid_index ON user_table (user_account_state_table_uuid);
        END IF; END $$;

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_table_hr_user_account_state_table_uuid_index') THEN
        CREATE INDEX user_table_hr_user_account_state_table_uuid_index ON user_table (hr_user_account_state_table_uuid);
        END IF; END $$;
      EOS
    end

    context '#stage_table' do
      let(:target) { registry.postgres.user_table.stage_table }

      it 'should render table template' do
        is_expected.to eq <<-EOS.strip_heredoc
          CREATE TEMPORARY TABLE IF NOT EXISTS user_table_stage
          (
            uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            user_account_state_table_uuid UUID DEFAULT default_user_account_state_table_uuid(),
            hr_user_account_state_table_uuid UUID,
            name VARCHAR
          );

          CREATE INDEX user_table_stage_user_account_state_table_uuid_index ON user_table_stage (user_account_state_table_uuid);
          CREATE INDEX user_table_stage_hr_user_account_state_table_uuid_index ON user_table_stage (hr_user_account_state_table_uuid);
        EOS
      end
    end
  end

  context '#as_file' do
    before do
      registry.schema :postgres do
        table 'user_account_state' do
          column 'name', type: :string, unique: true
          column 'description', type: :string
        end

        table 'user' do
          references :user_account_state
          references :user_account_state, label: 'hr'
          column 'name', type: :string
        end
      end
    end

    let(:table) { registry.postgres.user_table }
    let(:file) { table.as_file(columns) }
    let(:target) { file.as_table(table) }

    context 'without specified columns' do
      let(:columns) { [] }

      it 'should render table template' do
        is_expected.to eq <<-EOS.strip_heredoc
          CREATE TEMPORARY TABLE IF NOT EXISTS user_table_file
          (
            uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            user_account_state_table_uuid UUID,
            hr_user_account_state_table_uuid UUID,
            name VARCHAR
          );

          CREATE INDEX user_table_file_user_account_state_table_uuid_index ON user_table_file (user_account_state_table_uuid);
          CREATE INDEX user_table_file_hr_user_account_state_table_uuid_index ON user_table_file (hr_user_account_state_table_uuid);
        EOS
      end
    end

    context 'for postgres table with all specified columns' do
      let(:columns) { %w(uuid hr_user_account_state_table_uuid user_account_state_table_uuid name) }

      it 'should render table template' do
        is_expected.to eq <<-EOS.strip_heredoc
          CREATE TEMPORARY TABLE IF NOT EXISTS user_table_file
          (
            uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            hr_user_account_state_table_uuid UUID,
            user_account_state_table_uuid UUID,
            name VARCHAR
          );

          CREATE INDEX user_table_file_hr_user_account_state_table_uuid_index ON user_table_file (hr_user_account_state_table_uuid);
          CREATE INDEX user_table_file_user_account_state_table_uuid_index ON user_table_file (user_account_state_table_uuid);
        EOS
      end
    end

    context 'for postgres table with all specified columns in denormalized form' do
      let(:columns) { %w(uuid hr_user_account_state.name user_account_state.name name) }

      it 'should render table template' do
        is_expected.to eq <<-EOS.strip_heredoc
          CREATE TEMPORARY TABLE IF NOT EXISTS user_table_file
          (
            uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            hr_user_account_state_table_name VARCHAR,
            user_account_state_table_name VARCHAR,
            name VARCHAR
          );
        EOS
      end
    end
  end
end
