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

describe Masamune::Transform::DenormalizeTable do
  before do
    catalog.schema :postgres do
      dimension 'cluster', type: :mini do
        column 'id', type: :sequence, surrogate_key: true, auto: true
        column 'name', type: :string

        row name: 'current_database()', attributes: {default: true}
      end

      dimension 'date', type: :date do
        column 'date_id', type: :integer, unique: true, index: true, natural_key: true
      end

      dimension 'tenant', type: :two do
        references :cluster, natural_key: true
        column 'tenant_id', type: :integer, index: true, natural_key: true
      end

      dimension 'user', type: :two do
        column 'tenant_id', type: :integer, index: true, natural_key: true
        column 'user_id', type: :integer, index: true, natural_key: true
        column 'name', type: :string
      end

      dimension 'user_agent', type: :mini do
        column 'name', type: :string, unique: true, index: 'shared'
        column 'version', type: :string, unique: true, index: 'shared', default: 'Unknown'
        column 'mobile', type: :boolean, unique: true, index: 'shared', default: false
        column 'description', type: :string, null: true, ignore: true
      end

      fact 'visits', partition: 'y%Ym%m' do
        references :cluster
        references :date
        references :tenant
        references :user, label: 'manager'
        references :user
        references :user_agent
        measure 'total', type: :integer
      end
    end
  end

  let(:options) { {} }

  subject(:result) { transform.denormalize_table(target, options).to_s }

  context 'with postgres fact without :columns' do
    let(:target) { catalog.postgres.visits_fact }
    let(:options) { { } }

    it 'should eq render denormalize_table template' do
      is_expected.to eq <<-EOS.strip_heredoc
      SELECT
        cluster_type.name AS cluster_type_name,
        date_dimension.date_id AS date_dimension_date_id,
        tenant_dimension.tenant_id AS tenant_dimension_tenant_id,
        manager_user_dimension.tenant_id AS manager_user_dimension_tenant_id,
        manager_user_dimension.user_id AS manager_user_dimension_user_id,
        user_dimension.tenant_id AS user_dimension_tenant_id,
        user_dimension.user_id AS user_dimension_user_id,
        user_agent_type.name AS user_agent_type_name,
        user_agent_type.version AS user_agent_type_version,
        user_agent_type.mobile AS user_agent_type_mobile,
        visits_fact.total,
        visits_fact.time_key
      FROM
        visits_fact
      LEFT JOIN
        cluster_type
      ON
        cluster_type.id = visits_fact.cluster_type_id
      LEFT JOIN
        date_dimension
      ON
        date_dimension.id = visits_fact.date_dimension_id
      LEFT JOIN
        tenant_dimension
      ON
        tenant_dimension.id = visits_fact.tenant_dimension_id
      LEFT JOIN
        user_dimension AS manager_user_dimension
      ON
        manager_user_dimension.id = visits_fact.manager_user_dimension_id
      LEFT JOIN
        user_dimension
      ON
        user_dimension.id = visits_fact.user_dimension_id
      LEFT JOIN
        user_agent_type
      ON
        user_agent_type.id = visits_fact.user_agent_type_id
      ORDER BY
        cluster_type_name,
        date_dimension_date_id,
        tenant_dimension_tenant_id,
        manager_user_dimension_tenant_id,
        manager_user_dimension_user_id,
        user_dimension_tenant_id,
        user_dimension_user_id,
        user_agent_type_name,
        user_agent_type_version,
        user_agent_type_mobile,
        total,
        time_key
      ;
      EOS
    end
  end
  context 'with postgres fact with :columns' do
    let(:target) { catalog.postgres.visits_fact }
    let(:options) do
      {
        columns: [
          'date.date_id',
          'tenant.tenant_id',
          'manager_user.tenant_id',
          'manager_user.user_id',
          'user.tenant_id',
          'user.user_id',
          'user_agent.name',
          'user_agent.version',
          'total',
          'time_key'
        ]
      }
    end

    it 'should eq render denormalize_table template' do
      is_expected.to eq <<-EOS.strip_heredoc
      SELECT
        date_dimension.date_id AS date_dimension_date_id,
        tenant_dimension.tenant_id AS tenant_dimension_tenant_id,
        manager_user_dimension.tenant_id AS manager_user_dimension_tenant_id,
        manager_user_dimension.user_id AS manager_user_dimension_user_id,
        user_dimension.tenant_id AS user_dimension_tenant_id,
        user_dimension.user_id AS user_dimension_user_id,
        user_agent_type.name AS user_agent_type_name,
        user_agent_type.version AS user_agent_type_version,
        visits_fact.total,
        visits_fact.time_key
      FROM
        visits_fact
      LEFT JOIN
        date_dimension
      ON
        date_dimension.id = visits_fact.date_dimension_id
      LEFT JOIN
        tenant_dimension
      ON
        tenant_dimension.id = visits_fact.tenant_dimension_id
      LEFT JOIN
        user_dimension AS manager_user_dimension
      ON
        manager_user_dimension.id = visits_fact.manager_user_dimension_id
      LEFT JOIN
        user_dimension
      ON
        user_dimension.id = visits_fact.user_dimension_id
      LEFT JOIN
        user_agent_type
      ON
        user_agent_type.id = visits_fact.user_agent_type_id
      ORDER BY
        date_dimension_date_id,
        tenant_dimension_tenant_id,
        manager_user_dimension_tenant_id,
        manager_user_dimension_user_id,
        user_dimension_tenant_id,
        user_dimension_user_id,
        user_agent_type_name,
        user_agent_type_version,
        total,
        time_key
      ;
      EOS
    end
  end

  context 'with postgres fact with :except' do
    let(:target) { catalog.postgres.visits_fact }
    let(:options) do
      {
        except: [
          'cluster.name'
        ]
      }
    end

    it 'should eq render denormalize_table template' do
      is_expected.to eq <<-EOS.strip_heredoc
      SELECT
        date_dimension.date_id AS date_dimension_date_id,
        tenant_dimension.tenant_id AS tenant_dimension_tenant_id,
        manager_user_dimension.tenant_id AS manager_user_dimension_tenant_id,
        manager_user_dimension.user_id AS manager_user_dimension_user_id,
        user_dimension.tenant_id AS user_dimension_tenant_id,
        user_dimension.user_id AS user_dimension_user_id,
        user_agent_type.name AS user_agent_type_name,
        user_agent_type.version AS user_agent_type_version,
        user_agent_type.mobile AS user_agent_type_mobile,
        visits_fact.total,
        visits_fact.time_key
      FROM
        visits_fact
      LEFT JOIN
        date_dimension
      ON
        date_dimension.id = visits_fact.date_dimension_id
      LEFT JOIN
        tenant_dimension
      ON
        tenant_dimension.id = visits_fact.tenant_dimension_id
      LEFT JOIN
        user_dimension AS manager_user_dimension
      ON
        manager_user_dimension.id = visits_fact.manager_user_dimension_id
      LEFT JOIN
        user_dimension
      ON
        user_dimension.id = visits_fact.user_dimension_id
      LEFT JOIN
        user_agent_type
      ON
        user_agent_type.id = visits_fact.user_agent_type_id
      ORDER BY
        date_dimension_date_id,
        tenant_dimension_tenant_id,
        manager_user_dimension_tenant_id,
        manager_user_dimension_user_id,
        user_dimension_tenant_id,
        user_dimension_user_id,
        user_agent_type_name,
        user_agent_type_version,
        user_agent_type_mobile,
        total,
        time_key
      ;
      EOS
    end
  end

  context 'with hive table' do
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

    let(:target) { catalog.hive.tenant_dimension }

    let(:options) do
      {
        columns: [
          'tenant_id',
          'tenant_account_state',
          'tenant_premium_state',
          'preferences',
          'y',
          'm'
        ],
        order: [
          'tenant_id',
          'start_at'
        ]
      }
    end

    it 'should eq render denormalize_table template' do
      is_expected.to eq <<-EOS.strip_heredoc
        SELECT
          tenant_ledger.tenant_id,
          tenant_ledger.tenant_account_state,
          tenant_ledger.tenant_premium_state,
          tenant_ledger.preferences,
          tenant_ledger.y,
          tenant_ledger.m
        FROM
          tenant_ledger
        ORDER BY
          tenant_id,
          start_at
        ;
      EOS
    end
  end

  context 'with hive table with implicit references' do
    before do
      catalog.schema :hive do
        dimension 'date', type: :date, implicit: true do
          column 'date_id', type: :integer, natural_key: true
        end

        fact 'visits' do
          references :date
          references :user, degenerate: true
          measure 'total'
        end
      end
    end

    let(:target) { catalog.hive.visits_fact }

    let(:options) do
      {
        columns: [
          'date.date_id',
          'user.id',
          'total'
        ]
      }
    end

    it 'should eq render denormalize_table template' do
      is_expected.to eq <<-EOS.strip_heredoc
        SELECT
          date_dimension_date_id AS date_dimension_date_id,
          user_type_id AS user_type_id,
          visits_fact.total
        FROM
          visits_fact
        ORDER BY
          date_dimension_date_id,
          user_type_id,
          total
        ;
      EOS
    end
  end
end
