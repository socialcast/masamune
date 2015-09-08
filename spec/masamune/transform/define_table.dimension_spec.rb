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

  context 'for hive ledger dimension' do
    before do
      catalog.schema :hive do
        dimension 'tenant', type: :ledger do
          column 'tenant_id', type: :integer, natural_key: true
          column 'tenant_account_state', type: :enum, values: %w(missing unknown active inactive)
          column 'tenant_premium_state', type: :enum, values: %w(missing unkown goodwill pilot sandbox premium internal free vmware)
          column 'preferences', type: :key_value, null: true
        end
      end
    end

    let(:table) { catalog.hive.tenant_dimension }

    it 'should render table template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TABLE IF NOT EXISTS tenant_ledger
        (
          id STRING,
          tenant_id INT,
          tenant_account_state STRING,
          tenant_premium_state STRING,
          preferences STRING,
          source_kind STRING,
          source_uuid STRING,
          start_at STRING,
          last_modified_at STRING,
          delta INT
        )
        TBLPROPERTIES ('serialization.null.format' = '');
      EOS
    end
  end

  context 'for hive ledger dimension with partitions' do
    before do
      catalog.schema :hive do
        dimension 'tenant', type: :ledger do
          partition :y
          partition :m
          column 'tenant_id', type: :integer, natural_key: true
          column 'tenant_account_state', type: :enum, values: %w(missing unknown active inactive)
          column 'tenant_premium_state', type: :enum, values: %w(missing unkown goodwill pilot sandbox premium internal free vmware)
          column 'preferences', type: :key_value, null: true
        end
      end
    end

    let(:table) { catalog.hive.tenant_dimension }

    it 'should render table template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TABLE IF NOT EXISTS tenant_ledger
        (
          id STRING,
          tenant_id INT,
          tenant_account_state STRING,
          tenant_premium_state STRING,
          preferences STRING,
          source_kind STRING,
          source_uuid STRING,
          start_at STRING,
          last_modified_at STRING,
          delta INT
        )
        PARTITIONED BY (y INT, m INT)
        TBLPROPERTIES ('serialization.null.format' = '');
      EOS
    end
  end

  context 'for hive ledger dimension with :tsv format' do
    before do
      catalog.schema :hive do
        dimension 'tenant', type: :ledger, properties: { format: :tsv } do
          column 'tenant_id', type: :integer, natural_key: true
        end
      end
    end

    let(:table) { catalog.hive.tenant_dimension }

    it 'should render table template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TABLE IF NOT EXISTS tenant_ledger
        (
          id STRING,
          tenant_id INT,
          source_kind STRING,
          source_uuid STRING,
          start_at STRING,
          last_modified_at STRING,
          delta INT
        )
        ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
        TBLPROPERTIES ('serialization.null.format' = '');
      EOS
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
          id SERIAL,
          tenant_id INTEGER NOT NULL,
          user_id INTEGER NOT NULL,
          last_modified_at TIMESTAMP NOT NULL DEFAULT NOW()
        );

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_dimension_pkey') THEN
        ALTER TABLE user_dimension ADD PRIMARY KEY (id);
        END IF; END $$;
      EOS
    end
  end

  context 'for postgres dimension type: two' do
    before do
      catalog.schema :postgres do
        dimension 'user', type: :two do
          column 'tenant_id', natural_key: true
          column 'user_id', natural_key: true
        end
      end
    end

    let(:table) { catalog.postgres.user_dimension }

    it 'should render table template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TABLE IF NOT EXISTS user_dimension
        (
          id SERIAL,
          tenant_id INTEGER NOT NULL,
          user_id INTEGER NOT NULL,
          start_at TIMESTAMP NOT NULL DEFAULT TO_TIMESTAMP(0),
          end_at TIMESTAMP,
          version INTEGER DEFAULT 1,
          last_modified_at TIMESTAMP NOT NULL DEFAULT NOW()
        );

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_dimension_pkey') THEN
        ALTER TABLE user_dimension ADD PRIMARY KEY (id);
        END IF; END $$;

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_dimension_e6c3d91_key') THEN
        ALTER TABLE user_dimension ADD CONSTRAINT user_dimension_e6c3d91_key UNIQUE(tenant_id, user_id, start_at);
        END IF; END $$;

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_dimension_e6c3d91_index') THEN
        CREATE UNIQUE INDEX user_dimension_e6c3d91_index ON user_dimension (tenant_id, user_id, start_at);
        END IF; END $$;
      EOS
    end
  end

  context 'for postgres dimension type: four' do
    before do
      catalog.schema :postgres do
        dimension 'cluster', type: :mini do
          column 'id', type: :integer, surrogate_key: true, auto: true
          column 'name', type: :string, unique: true
          row name: 'default', attributes: {default: true}
        end

        dimension 'user_account_state', type: :mini do
          column 'name', type: :string, unique: true
          column 'description', type: :string
          row name: 'active', description: 'Active', attributes: {default: true}
        end

        dimension 'user', type: :four do
          references :cluster
          references :user_account_state
          column 'tenant_id', natural_key: true
          column 'user_id', natural_key: true
          column 'preferences', type: :key_value, null: true
        end
      end
    end

    let(:table) { catalog.postgres.user_dimension }

    it 'should render table template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TABLE IF NOT EXISTS user_dimension_ledger
        (
          id SERIAL,
          cluster_type_id INTEGER NOT NULL DEFAULT default_cluster_type_id(),
          user_account_state_type_id INTEGER,
          tenant_id INTEGER NOT NULL,
          user_id INTEGER NOT NULL,
          preferences HSTORE,
          source_kind VARCHAR NOT NULL,
          source_uuid VARCHAR NOT NULL,
          start_at TIMESTAMP NOT NULL,
          last_modified_at TIMESTAMP NOT NULL DEFAULT NOW(),
          delta INTEGER NOT NULL
        );

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_dimension_ledger_pkey') THEN
        ALTER TABLE user_dimension_ledger ADD PRIMARY KEY (cluster_type_id, id);
        END IF; END $$;

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conname = 'user_dimension_ledger_d6b9b38_fkey') THEN
        ALTER TABLE user_dimension_ledger ADD CONSTRAINT user_dimension_ledger_d6b9b38_fkey FOREIGN KEY (cluster_type_id) REFERENCES cluster_type(id);
        END IF; END $$;

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conname = 'user_dimension_ledger_7988187_fkey') THEN
        ALTER TABLE user_dimension_ledger ADD CONSTRAINT user_dimension_ledger_7988187_fkey FOREIGN KEY (user_account_state_type_id) REFERENCES user_account_state_type(id);
        END IF; END $$;

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_dimension_ledger_ff54dba_key') THEN
        ALTER TABLE user_dimension_ledger ADD CONSTRAINT user_dimension_ledger_ff54dba_key UNIQUE(cluster_type_id, tenant_id, user_id, source_kind, source_uuid, start_at);
        END IF; END $$;

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_dimension_ledger_370d6dd_index') THEN
        CREATE INDEX user_dimension_ledger_370d6dd_index ON user_dimension_ledger (tenant_id, user_id, source_kind, source_uuid, start_at);
        END IF; END $$;

        CREATE TABLE IF NOT EXISTS user_dimension
        (
          id SERIAL,
          cluster_type_id INTEGER NOT NULL DEFAULT default_cluster_type_id(),
          user_account_state_type_id INTEGER NOT NULL DEFAULT default_user_account_state_type_id(),
          tenant_id INTEGER NOT NULL,
          user_id INTEGER NOT NULL,
          preferences HSTORE,
          parent_id INTEGER,
          record_id INTEGER,
          start_at TIMESTAMP NOT NULL DEFAULT TO_TIMESTAMP(0),
          end_at TIMESTAMP,
          version INTEGER DEFAULT 1,
          last_modified_at TIMESTAMP NOT NULL DEFAULT NOW()
        );

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_dimension_pkey') THEN
        ALTER TABLE user_dimension ADD PRIMARY KEY (cluster_type_id, id);
        END IF; END $$;

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conname = 'user_dimension_d6b9b38_fkey') THEN
        ALTER TABLE user_dimension ADD CONSTRAINT user_dimension_d6b9b38_fkey FOREIGN KEY (cluster_type_id) REFERENCES cluster_type(id);
        END IF; END $$;

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conname = 'user_dimension_7988187_fkey') THEN
        ALTER TABLE user_dimension ADD CONSTRAINT user_dimension_7988187_fkey FOREIGN KEY (user_account_state_type_id) REFERENCES user_account_state_type(id);
        END IF; END $$;

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conname = 'user_dimension_e0538bc_fkey') THEN
        ALTER TABLE user_dimension ADD CONSTRAINT user_dimension_e0538bc_fkey FOREIGN KEY (cluster_type_id, parent_id) REFERENCES user_dimension_ledger(cluster_type_id, id);
        END IF; END $$;

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conname = 'user_dimension_824002d_fkey') THEN
        ALTER TABLE user_dimension ADD CONSTRAINT user_dimension_824002d_fkey FOREIGN KEY (cluster_type_id, record_id) REFERENCES user_dimension_ledger(cluster_type_id, id);
        END IF; END $$;

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_dimension_3fcebfa_key') THEN
        ALTER TABLE user_dimension ADD CONSTRAINT user_dimension_3fcebfa_key UNIQUE(cluster_type_id, tenant_id, user_id, start_at);
        END IF; END $$;

        DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'user_dimension_e6c3d91_index') THEN
        CREATE INDEX user_dimension_e6c3d91_index ON user_dimension (tenant_id, user_id, start_at);
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
          column 'tenant_id', natural_key: true
          column 'user_id', natural_key: true
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
          parent_id INTEGER,
          record_id INTEGER,
          start_at TIMESTAMP DEFAULT TO_TIMESTAMP(0),
          end_at TIMESTAMP,
          version INTEGER DEFAULT 1,
          last_modified_at TIMESTAMP DEFAULT NOW()
        );

        CREATE INDEX user_consolidated_forward_dimension_stage_e6c3d91_index ON user_consolidated_forward_dimension_stage (tenant_id, user_id, start_at);
      EOS
    end
  end
end
