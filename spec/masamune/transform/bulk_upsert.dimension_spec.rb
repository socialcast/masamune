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

describe Masamune::Transform::BulkUpsert do
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
        row name: 'registered', description: 'Registered'
        row name: 'active', description: 'Active', attributes: {default: true}
        row name: 'inactive', description: 'Inactive'
      end

      dimension 'department', type: :mini do
        references :cluster
        column 'tenant_id', type: :integer, unique: true, natural_key: true
        column 'department_id', type: :integer, unique: true, natural_key: true
        row tenant_id: -1, department_id: -1, attributes: {default: true}
      end

      dimension 'user', type: :four do
        references :cluster
        references :department, insert: true
        references :user_account_state
        references :user_account_state, label: :hr
        column 'tenant_id', index: true, natural_key: true
        column 'user_id', index: true, natural_key: true
        column 'name', type: :string
        column 'preferences', type: :key_value, null: true
      end

      file 'user' do
        column 'tenant_id', type: :integer
        column 'user_id', type: :integer
        column 'department.department_id', type: :integer
        column 'user_account_state.name', type: :string
        column 'hr_user_account_state.name', type: :string
        column 'preferences', type: :json
        column 'start_at', type: :timestamp
        column 'source_kind', type: :string
        column 'delta', type: :integer
      end
    end
  end

  let(:target) { catalog.postgres.user_dimension }

  context 'for postgres dimension' do
    subject(:result) { transform.bulk_upsert(target.stage_table, target).to_s }

    it 'should render bulk_upsert template' do
      is_expected.to eq <<-EOS.strip_heredoc
        BEGIN;
        LOCK TABLE user_dimension IN EXCLUSIVE MODE;

        UPDATE
          user_dimension
        SET
          department_type_id = COALESCE(user_dimension_stage.department_type_id, user_dimension.department_type_id),
          user_account_state_type_id = COALESCE(user_dimension_stage.user_account_state_type_id, user_dimension.user_account_state_type_id),
          hr_user_account_state_type_id = COALESCE(user_dimension_stage.hr_user_account_state_type_id, user_dimension.hr_user_account_state_type_id),
          name = COALESCE(user_dimension_stage.name, user_dimension.name),
          preferences = COALESCE(user_dimension_stage.preferences, user_dimension.preferences)
        FROM
          user_dimension_stage
        WHERE
          user_dimension.tenant_id = user_dimension_stage.tenant_id AND
          user_dimension.user_id = user_dimension_stage.user_id AND
          user_dimension.start_at = user_dimension_stage.start_at
        ;

        INSERT INTO
          user_dimension (department_type_id, user_account_state_type_id, hr_user_account_state_type_id, tenant_id, user_id, name, preferences, parent_id, record_id, start_at, end_at, version, last_modified_at)
        SELECT
          user_dimension_stage.department_type_id,
          user_dimension_stage.user_account_state_type_id,
          user_dimension_stage.hr_user_account_state_type_id,
          user_dimension_stage.tenant_id,
          user_dimension_stage.user_id,
          user_dimension_stage.name,
          user_dimension_stage.preferences,
          user_dimension_stage.parent_id,
          user_dimension_stage.record_id,
          user_dimension_stage.start_at,
          user_dimension_stage.end_at,
          user_dimension_stage.version,
          user_dimension_stage.last_modified_at
        FROM
          user_dimension_stage
        LEFT OUTER JOIN
          user_dimension
        ON
          user_dimension.tenant_id = user_dimension_stage.tenant_id AND
          user_dimension.user_id = user_dimension_stage.user_id AND
          user_dimension.start_at = user_dimension_stage.start_at
        WHERE
          user_dimension.tenant_id IS NULL AND
          user_dimension.user_id IS NULL AND
          user_dimension.start_at IS NULL
        ;

        COMMIT;
      EOS
    end
  end

  context 'for postgres dimension ledger' do
    subject(:result) { transform.bulk_upsert(target.ledger_table.stage_table, target.ledger_table).to_s }

    it 'should render bulk_upsert template' do
      is_expected.to eq <<-EOS.strip_heredoc
        BEGIN;
        LOCK TABLE user_dimension_ledger IN EXCLUSIVE MODE;

        UPDATE
          user_dimension_ledger
        SET
          department_type_id = COALESCE(user_dimension_ledger_stage.department_type_id, user_dimension_ledger.department_type_id),
          user_account_state_type_id = COALESCE(user_dimension_ledger_stage.user_account_state_type_id, user_dimension_ledger.user_account_state_type_id),
          hr_user_account_state_type_id = COALESCE(user_dimension_ledger_stage.hr_user_account_state_type_id, user_dimension_ledger.hr_user_account_state_type_id),
          name = COALESCE(user_dimension_ledger_stage.name, user_dimension_ledger.name),
          preferences = COALESCE(user_dimension_ledger_stage.preferences, user_dimension_ledger.preferences)
        FROM
          user_dimension_ledger_stage
        WHERE
          user_dimension_ledger.tenant_id = user_dimension_ledger_stage.tenant_id AND
          user_dimension_ledger.user_id = user_dimension_ledger_stage.user_id AND
          user_dimension_ledger.source_kind = user_dimension_ledger_stage.source_kind AND
          user_dimension_ledger.source_uuid = user_dimension_ledger_stage.source_uuid AND
          user_dimension_ledger.start_at = user_dimension_ledger_stage.start_at
        ;

        INSERT INTO
          user_dimension_ledger (department_type_id, user_account_state_type_id, hr_user_account_state_type_id, tenant_id, user_id, name, preferences, source_kind, source_uuid, start_at, last_modified_at, delta)
        SELECT
          user_dimension_ledger_stage.department_type_id,
          user_dimension_ledger_stage.user_account_state_type_id,
          user_dimension_ledger_stage.hr_user_account_state_type_id,
          user_dimension_ledger_stage.tenant_id,
          user_dimension_ledger_stage.user_id,
          user_dimension_ledger_stage.name,
          user_dimension_ledger_stage.preferences,
          user_dimension_ledger_stage.source_kind,
          user_dimension_ledger_stage.source_uuid,
          user_dimension_ledger_stage.start_at,
          user_dimension_ledger_stage.last_modified_at,
          user_dimension_ledger_stage.delta
        FROM
          user_dimension_ledger_stage
        LEFT OUTER JOIN
          user_dimension_ledger
        ON
          user_dimension_ledger.tenant_id = user_dimension_ledger_stage.tenant_id AND
          user_dimension_ledger.user_id = user_dimension_ledger_stage.user_id AND
          user_dimension_ledger.source_kind = user_dimension_ledger_stage.source_kind AND
          user_dimension_ledger.source_uuid = user_dimension_ledger_stage.source_uuid AND
          user_dimension_ledger.start_at = user_dimension_ledger_stage.start_at
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
