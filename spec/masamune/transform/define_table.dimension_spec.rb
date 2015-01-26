require 'spec_helper'

describe 'Masamune::Transform::DefineTable with Masamune::Schema::Dimension' do
  let(:transform) { Object.new.extend(Masamune::Transform::DefineTable) }

  subject { transform.define_table(dimension).to_s }

  context 'for type :one' do
    let(:dimension) do
      Masamune::Schema::Dimension.new id: 'user', type: :one,
        columns: [
          Masamune::Schema::Column.new(id: 'tenant_id'),
          Masamune::Schema::Column.new(id: 'user_id')
        ]
    end

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

  context 'for type :two' do
    let(:dimension) do
      Masamune::Schema::Dimension.new id: 'user', type: :two,
        columns: [
          Masamune::Schema::Column.new(id: 'tenant_id', index: true, natural_key: true),
          Masamune::Schema::Column.new(id: 'user_id', index: true, natural_key: true)
        ]
    end

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

  context 'for type :four' do
    let(:mini_dimension) do
      Masamune::Schema::Dimension.new id: 'user_account_state', type: :mini,
        columns: [
          Masamune::Schema::Column.new(id: 'name', type: :string, unique: true),
          Masamune::Schema::Column.new(id: 'description', type: :string)
        ],
        rows: [
          Masamune::Schema::Row.new(values: {
            name: 'active',
            description: 'Active',
          }, default: true)
        ]
    end

    let(:dimension) do
      Masamune::Schema::Dimension.new id: 'user', type: :four, references: [Masamune::Schema::TableReference.new(mini_dimension)],
        columns: [
          Masamune::Schema::Column.new(id: 'tenant_id', index: true, natural_key: true),
          Masamune::Schema::Column.new(id: 'user_id', index: true, natural_key: true),
          Masamune::Schema::Column.new(id: 'preferences', type: :key_value, null: true)
        ]
    end

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
end
