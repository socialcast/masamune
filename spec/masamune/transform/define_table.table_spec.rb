#  The MIT License (MIT)
#
#  Copyright (c) 2014-2015, VMware, Inc. All Rights Reserved.
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in
#  all copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
#  THE SOFTWARE.

require 'spec_helper'

describe Masamune::Transform::DefineTable do
  subject { transform.define_table(target).to_s }

  context 'for postgres table with columns' do
    before do
      catalog.schema :postgres do
        table 'user' do
          column 'tenant_id'
          column 'user_id'
        end
      end
    end

    let(:target) { catalog.postgres.user_table }

    it 'should render table template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TABLE IF NOT EXISTS user_table
        (
          id SERIAL PRIMARY KEY,
          tenant_id INTEGER NOT NULL,
          user_id INTEGER NOT NULL
        );
      EOS
    end
  end

  context 'for postgres table with index columns' do
    before do
      catalog.schema :postgres do
        table 'user' do
          column 'tenant_id', index: true
          column 'user_id', index: true
        end
      end
    end

    let(:target) { catalog.postgres.user_table }

    it 'should render table template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TABLE IF NOT EXISTS user_table
        (
          id SERIAL PRIMARY KEY,
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
      catalog.schema :postgres do
        table 'user' do
          column 'tenant_id', index: ['tenant_id', 'shared']
          column 'user_id', index: ['user_id', 'shared']
        end
      end
    end

    let(:target) { catalog.postgres.user_table }

    it 'should render table template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TABLE IF NOT EXISTS user_table
        (
          id SERIAL PRIMARY KEY,
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
      catalog.schema :postgres do
        table 'user' do
          column 'tenant_id', unique: ['shared']
          column 'user_id', unique: ['user_id', 'shared']
        end
      end
    end

    let(:target) { catalog.postgres.user_table }

    it 'should render table template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TABLE IF NOT EXISTS user_table
        (
          id SERIAL PRIMARY KEY,
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
      catalog.schema :postgres do
        table 'user' do
          column 'tenant_id'
          column 'user_id'
          column 'state', type: :enum, sub_type: :user_state, values: %w(active inactive terminated), default: 'active'
        end
      end
    end

    let(:target) { catalog.postgres.user_table }

    it 'should render table template' do
      is_expected.to eq <<-EOS.strip_heredoc
        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_type t WHERE LOWER(t.typname) = LOWER('USER_STATE_TYPE')) THEN
        CREATE TYPE USER_STATE_TYPE AS ENUM ('active', 'inactive', 'terminated');
        END IF; END $$;

        CREATE TABLE IF NOT EXISTS user_table
        (
          id SERIAL PRIMARY KEY,
          tenant_id INTEGER NOT NULL,
          user_id INTEGER NOT NULL,
          state USER_STATE_TYPE NOT NULL DEFAULT 'active'::USER_STATE_TYPE
        );
      EOS
    end
  end

  context 'for postgres table with surrogate_key columns override' do
    before do
      catalog.schema :postgres do
        table 'user' do
          column 'identifier', type: :uuid, surrogate_key: true
          column 'name', type: :string
        end
      end
    end

    let(:target) { catalog.postgres.user_table }

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
      catalog.schema :postgres do
        table 'user' do
          column 'name', type: :string
          column 'description', type: :string
          row name: 'registered', description: 'Registered'
          row name: 'active'
        end
      end
    end

    let(:target) { catalog.postgres.user_table }

    it 'should render table template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TABLE IF NOT EXISTS user_table
        (
          id SERIAL PRIMARY KEY,
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
      catalog.schema :postgres do
        table 'user' do
          column 'tenant_id', unique: 'tenant_and_user', index: 'tenant_and_user'
          column 'user_id', unique: 'tenant_and_user', index: 'tenant_and_user'
        end
      end
    end

    let(:target) { catalog.postgres.user_table }

    it 'should render table template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TABLE IF NOT EXISTS user_table
        (
          id SERIAL PRIMARY KEY,
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
      catalog.schema :postgres do
        table 'user' do
          column 'tenant_id', type: :integer, natural_key: true
          column 'user_id', type: :integer, natural_key: true
          row tenant_id: 'default_tenant_id()', user_id: -1, attributes: {default: true}
          row tenant_id: 'default_tenant_id()', user_id: -2, attributes: {id: 'unknown'}
        end
      end
    end

    let(:target) { catalog.postgres.user_table }

    it 'should render table template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TABLE IF NOT EXISTS user_table
        (
          id SERIAL PRIMARY KEY,
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

        CREATE OR REPLACE FUNCTION default_user_table_id()
        RETURNS INTEGER IMMUTABLE AS $$
          SELECT id FROM user_table WHERE tenant_id = default_tenant_id() AND user_id = -1;
        $$ LANGUAGE SQL;

        CREATE OR REPLACE FUNCTION unknown_user_id()
        RETURNS INTEGER IMMUTABLE AS $$
          SELECT -2;
        $$ LANGUAGE SQL;

        CREATE OR REPLACE FUNCTION unknown_user_table_id()
        RETURNS INTEGER IMMUTABLE AS $$
          SELECT id FROM user_table WHERE tenant_id = default_tenant_id() AND user_id = -2;
        $$ LANGUAGE SQL;
      EOS
    end
  end

  context 'for postgres table with referenced tables' do
    before do
      catalog.schema :postgres do
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

    let(:target) { catalog.postgres.user_table }

    it 'should render table template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TABLE IF NOT EXISTS user_table
        (
          id SERIAL PRIMARY KEY,
          user_account_state_table_id INTEGER NOT NULL REFERENCES user_account_state_table(id) DEFAULT default_user_account_state_table_id(),
          name VARCHAR NOT NULL
        );

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_table_user_account_state_table_id_index') THEN
        CREATE INDEX user_table_user_account_state_table_id_index ON user_table (user_account_state_table_id);
        END IF; END $$;
      EOS
    end
  end

  context 'for postgres table with labeled referenced table' do
    before do
      catalog.schema :postgres do
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

    let(:target) { catalog.postgres.user_table }

    it 'should render table template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TABLE IF NOT EXISTS user_table
        (
          id SERIAL PRIMARY KEY,
          user_account_state_table_id INTEGER NOT NULL REFERENCES user_account_state_table(id) DEFAULT default_user_account_state_table_id(),
          hr_user_account_state_table_id INTEGER REFERENCES user_account_state_table(id),
          name VARCHAR NOT NULL
        );

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_table_user_account_state_table_id_index') THEN
        CREATE INDEX user_table_user_account_state_table_id_index ON user_table (user_account_state_table_id);
        END IF; END $$;

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_table_hr_user_account_state_table_id_index') THEN
        CREATE INDEX user_table_hr_user_account_state_table_id_index ON user_table (hr_user_account_state_table_id);
        END IF; END $$;
      EOS
    end
  end

  context '#stage_table' do
    before do
      catalog.schema :postgres do
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

    let(:table) { catalog.postgres.user_table }
    let(:target) { table.stage_table(columns: columns) }

    context 'without specified columns' do
      let(:columns) { [] }

      it 'should render table template' do
        is_expected.to eq <<-EOS.strip_heredoc
          CREATE TEMPORARY TABLE IF NOT EXISTS user_table_stage
          (
            user_account_state_table_id INTEGER,
            hr_user_account_state_table_id INTEGER,
            name VARCHAR
          );

          CREATE INDEX user_table_stage_user_account_state_table_id_index ON user_table_stage (user_account_state_table_id);
          CREATE INDEX user_table_stage_hr_user_account_state_table_id_index ON user_table_stage (hr_user_account_state_table_id);
        EOS
      end
    end

    context 'for postgres table with all specified columns' do
      let(:columns) { %w(hr_user_account_state_table_id user_account_state_table_id name) }

      it 'should render table template' do
        is_expected.to eq <<-EOS.strip_heredoc
          CREATE TEMPORARY TABLE IF NOT EXISTS user_table_stage
          (
            hr_user_account_state_table_id INTEGER,
            user_account_state_table_id INTEGER,
            name VARCHAR
          );

          CREATE INDEX user_table_stage_hr_user_account_state_table_id_index ON user_table_stage (hr_user_account_state_table_id);
          CREATE INDEX user_table_stage_user_account_state_table_id_index ON user_table_stage (user_account_state_table_id);
        EOS
      end
    end

    context 'for postgres table with all specified columns in denormalized form' do
      let(:columns) { %w(hr_user_account_state.name user_account_state.name name) }

      it 'should render table template' do
        is_expected.to eq <<-EOS.strip_heredoc
          CREATE TEMPORARY TABLE IF NOT EXISTS user_table_stage
          (
            hr_user_account_state_table_name VARCHAR,
            user_account_state_table_name VARCHAR,
            name VARCHAR
          );
        EOS
      end
    end
  end

  context 'for postgres table with sequence column' do
    before do
      catalog.schema :postgres do
        table 'user' do
          column 'id', type: :sequence, surrogate_key: true, sequence_offset: 1024
          column 'tenant_id'
          column 'user_id'
        end
      end
    end

    let(:target) { catalog.postgres.user_table }

    it 'should render table template' do
      is_expected.to eq <<-EOS.strip_heredoc
        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_table_id_seq') THEN
        CREATE SEQUENCE user_table_id_seq;
        ALTER SEQUENCE user_table_id_seq RESTART 1024;
        END IF; END $$;

        CREATE TABLE IF NOT EXISTS user_table
        (
          id INTEGER PRIMARY KEY DEFAULT nextval('user_table_id_seq'),
          tenant_id INTEGER NOT NULL,
          user_id INTEGER NOT NULL
        );

        ALTER SEQUENCE user_table_id_seq OWNED BY user_table.id;
      EOS
    end
  end
end
