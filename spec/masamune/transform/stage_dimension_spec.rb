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

describe Masamune::Transform::StageDimension do
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

  let(:target) { catalog.postgres.user_dimension.ledger_table }
  let(:source) { catalog.postgres.user_file.stage_table(suffix: 'file', table: target, inherit: false) }

  context 'for postgres dimension' do
    subject(:result) { transform.stage_dimension(source, target).to_s }

    it 'should eq render stage_dimension template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TEMPORARY TABLE IF NOT EXISTS user_dimension_ledger_stage (LIKE user_dimension_ledger INCLUDING ALL);

        INSERT INTO
          user_dimension_ledger_stage (department_type_id, user_account_state_type_id, hr_user_account_state_type_id, tenant_id, user_id, preferences, source_kind, start_at, delta)
        SELECT
          department_type.id,
          user_account_state_type.id,
          hr_user_account_state_type.id,
          user_file_dimension_ledger_stage.tenant_id,
          user_file_dimension_ledger_stage.user_id,
          json_to_hstore(user_file_dimension_ledger_stage.preferences),
          user_file_dimension_ledger_stage.source_kind,
          user_file_dimension_ledger_stage.start_at,
          user_file_dimension_ledger_stage.delta
        FROM
          user_file_dimension_ledger_stage
        LEFT JOIN
          department_type AS department_type
        ON
          department_type.department_id = user_file_dimension_ledger_stage.department_type_department_id AND
          department_type.tenant_id = user_file_dimension_ledger_stage.tenant_id
        LEFT JOIN
          user_account_state_type AS user_account_state_type
        ON
          user_account_state_type.name = user_file_dimension_ledger_stage.user_account_state_type_name
        LEFT JOIN
          user_account_state_type AS hr_user_account_state_type
        ON
          hr_user_account_state_type.name = user_file_dimension_ledger_stage.hr_user_account_state_type_name
        ;

        ANALYZE user_dimension_ledger_stage;
      EOS
    end
  end
end
