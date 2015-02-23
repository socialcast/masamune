require 'spec_helper'

describe Masamune::Transform::DefineTable do
  subject { transform.define_table(table).to_s }

  context 'for hive implicit dimension' do
    before do
      catalog.schema :hive do
        dimension 'user', implicit: true do
          column 'user_id', natural_key: true
        end
      end
    end

    let(:table) { catalog.hive.user_dimension }

    it 'should not render table template' do
      is_expected.to eq ''
    end
  end

  context 'for postgres dimension type: one' do
    before do
      catalog.schema :postgres do
        dimension 'user', type: :one do
          column 'tenant_id'
          column 'user_id'
        end
      end
    end

    let(:table) { catalog.postgres.user_dimension }

    it 'should render table template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TABLE IF NOT EXISTS user_dimension
        (
          uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
          tenant_id INTEGER NOT NULL,
          user_id INTEGER NOT NULL,
          last_modified_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
      EOS
    end
  end

  context 'for postgres dimension type: two' do
    before do
      catalog.schema :postgres do
        dimension 'user', type: :two do
          column 'tenant_id', index: true, natural_key: true
          column 'user_id', index: true, natural_key: true
        end
      end
    end

    let(:table) { catalog.postgres.user_dimension }

    it 'should render table template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TABLE IF NOT EXISTS user_dimension
        (
          uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
          tenant_id INTEGER NOT NULL,
          user_id INTEGER NOT NULL,
          start_at TIMESTAMP NOT NULL DEFAULT TO_TIMESTAMP(0),
          end_at TIMESTAMP,
          version INTEGER DEFAULT 1,
          last_modified_at TIMESTAMP NOT NULL DEFAULT NOW()
        );

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_dimension_tenant_id_user_id_start_at_key') THEN
        ALTER TABLE user_dimension ADD CONSTRAINT user_dimension_tenant_id_user_id_start_at_key UNIQUE(tenant_id, user_id, start_at);
        END IF; END $$;

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_dimension_tenant_id_index') THEN
        CREATE INDEX user_dimension_tenant_id_index ON user_dimension (tenant_id);
        END IF; END $$;

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_dimension_user_id_index') THEN
        CREATE INDEX user_dimension_user_id_index ON user_dimension (user_id);
        END IF; END $$;

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_dimension_start_at_index') THEN
        CREATE INDEX user_dimension_start_at_index ON user_dimension (start_at);
        END IF; END $$;

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_dimension_end_at_index') THEN
        CREATE INDEX user_dimension_end_at_index ON user_dimension (end_at);
        END IF; END $$;

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_dimension_version_index') THEN
        CREATE INDEX user_dimension_version_index ON user_dimension (version);
        END IF; END $$;
      EOS
    end
  end

  context 'for postgres dimension type: four' do
    before do
      catalog.schema :postgres do
        dimension 'user_account_state', type: :mini do
          column 'name', type: :string, unique: true
          column 'description', type: :string
          row name: 'active', description: 'Active', attributes: {default: true}
        end

        dimension 'user', type: :four do
          references :user_account_state
          column 'tenant_id', index: true, natural_key: true
          column 'user_id', index: true, natural_key: true
          column 'preferences', type: :key_value, null: true
        end
      end
    end

    let(:table) { catalog.postgres.user_dimension }

    it 'should render table template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TABLE IF NOT EXISTS user_dimension_ledger
        (
          uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
          user_account_state_type_id INTEGER REFERENCES user_account_state_type(id) DEFAULT default_user_account_state_type_id(),
          tenant_id INTEGER NOT NULL,
          user_id INTEGER NOT NULL,
          preferences_now HSTORE,
          preferences_was HSTORE,
          source_kind VARCHAR NOT NULL,
          source_uuid VARCHAR NOT NULL,
          start_at TIMESTAMP NOT NULL,
          last_modified_at TIMESTAMP NOT NULL DEFAULT NOW(),
          delta INTEGER NOT NULL
        );

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_dimension_ledger_tenant_id_user_id_source_kind_source_uuid_start_at_key') THEN
        ALTER TABLE user_dimension_ledger ADD CONSTRAINT user_dimension_ledger_tenant_id_user_id_source_kind_source_uuid_start_at_key UNIQUE(tenant_id, user_id, source_kind, source_uuid, start_at);
        END IF; END $$;

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_dimension_ledger_user_account_state_type_id_index') THEN
        CREATE INDEX user_dimension_ledger_user_account_state_type_id_index ON user_dimension_ledger (user_account_state_type_id);
        END IF; END $$;

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_dimension_ledger_tenant_id_index') THEN
        CREATE INDEX user_dimension_ledger_tenant_id_index ON user_dimension_ledger (tenant_id);
        END IF; END $$;

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_dimension_ledger_user_id_index') THEN
        CREATE INDEX user_dimension_ledger_user_id_index ON user_dimension_ledger (user_id);
        END IF; END $$;

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_dimension_ledger_start_at_index') THEN
        CREATE INDEX user_dimension_ledger_start_at_index ON user_dimension_ledger (start_at);
        END IF; END $$;

        CREATE TABLE IF NOT EXISTS user_dimension
        (
          uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
          user_account_state_type_id INTEGER NOT NULL REFERENCES user_account_state_type(id) DEFAULT default_user_account_state_type_id(),
          tenant_id INTEGER NOT NULL,
          user_id INTEGER NOT NULL,
          preferences HSTORE,
          parent_uuid UUID REFERENCES user_dimension_ledger(uuid),
          record_uuid UUID REFERENCES user_dimension_ledger(uuid),
          start_at TIMESTAMP NOT NULL DEFAULT TO_TIMESTAMP(0),
          end_at TIMESTAMP,
          version INTEGER DEFAULT 1,
          last_modified_at TIMESTAMP NOT NULL DEFAULT NOW()
        );

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_dimension_tenant_id_user_id_start_at_key') THEN
        ALTER TABLE user_dimension ADD CONSTRAINT user_dimension_tenant_id_user_id_start_at_key UNIQUE(tenant_id, user_id, start_at);
        END IF; END $$;

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_dimension_user_account_state_type_id_index') THEN
        CREATE INDEX user_dimension_user_account_state_type_id_index ON user_dimension (user_account_state_type_id);
        END IF; END $$;

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_dimension_tenant_id_index') THEN
        CREATE INDEX user_dimension_tenant_id_index ON user_dimension (tenant_id);
        END IF; END $$;

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_dimension_user_id_index') THEN
        CREATE INDEX user_dimension_user_id_index ON user_dimension (user_id);
        END IF; END $$;

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_dimension_start_at_index') THEN
        CREATE INDEX user_dimension_start_at_index ON user_dimension (start_at);
        END IF; END $$;

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_dimension_end_at_index') THEN
        CREATE INDEX user_dimension_end_at_index ON user_dimension (end_at);
        END IF; END $$;

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_dimension_version_index') THEN
        CREATE INDEX user_dimension_version_index ON user_dimension (version);
        END IF; END $$;
      EOS
    end
  end

  context 'for postgres dimension type: four stage table' do
    before do
      catalog.schema :postgres do
        dimension 'user_account_state', type: :mini do
          column 'name', type: :string, unique: true
          column 'description', type: :string
          row name: 'active', description: 'Active', attributes: {default: true}
        end

        dimension 'user', type: :four do
          references :user_account_state
          column 'tenant_id', index: true, natural_key: true
          column 'user_id', index: true, natural_key: true
          column 'preferences', type: :key_value, null: true
        end
      end
    end

    let(:table) { catalog.postgres.user_dimension.stage_table(suffix: 'consolidated_forward') }

    it 'should render table template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TEMPORARY TABLE IF NOT EXISTS user_consolidated_forward_dimension_stage
        (
          user_account_state_type_id INTEGER DEFAULT default_user_account_state_type_id(),
          tenant_id INTEGER,
          user_id INTEGER,
          preferences HSTORE,
          parent_uuid UUID,
          record_uuid UUID,
          start_at TIMESTAMP DEFAULT TO_TIMESTAMP(0),
          end_at TIMESTAMP,
          version INTEGER DEFAULT 1,
          last_modified_at TIMESTAMP DEFAULT NOW()
        );

        CREATE INDEX user_consolidated_forward_dimension_stage_user_account_state_type_id_index ON user_consolidated_forward_dimension_stage (user_account_state_type_id);
        CREATE INDEX user_consolidated_forward_dimension_stage_tenant_id_index ON user_consolidated_forward_dimension_stage (tenant_id);
        CREATE INDEX user_consolidated_forward_dimension_stage_user_id_index ON user_consolidated_forward_dimension_stage (user_id);
        CREATE INDEX user_consolidated_forward_dimension_stage_start_at_index ON user_consolidated_forward_dimension_stage (start_at);
        CREATE INDEX user_consolidated_forward_dimension_stage_end_at_index ON user_consolidated_forward_dimension_stage (end_at);
        CREATE INDEX user_consolidated_forward_dimension_stage_version_index ON user_consolidated_forward_dimension_stage (version);
      EOS
    end
  end
end
