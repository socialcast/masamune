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

describe Masamune::Transform::InsertReferenceValues do
  before do
    catalog.schema :postgres do
      dimension 'department', type: :mini do
        column 'tenant_id', type: :integer, unique: true, natural_key: true
        column 'department_id', type: :integer, unique: true, natural_key: true
        row tenant_id: -1, department_id: -1, attributes: {default: true}
      end

      dimension 'user', type: :four do
        references :department, insert: true
        column 'tenant_id', index: true, natural_key: true
        column 'user_id', index: true, natural_key: true
        column 'name', type: :string
      end

      file 'user' do
        column 'tenant_id', type: :integer
        column 'user_id', type: :integer
        column 'department.department_id', type: :integer
        column 'start_at', type: :timestamp
        column 'source_kind', type: :string
        column 'delta', type: :integer
      end

      file 'misc' do
      end
    end
  end

  let(:target) { catalog.postgres.user_dimension.ledger_table }

  context 'for postgres dimension with file containing references' do
    let(:source) { catalog.postgres.user_file.stage_table(table: target) }
    subject(:result) { transform.insert_reference_values(source, target).to_s }

    it 'should render insert_reference_values template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TEMPORARY TABLE IF NOT EXISTS department_type_stage (LIKE department_type INCLUDING ALL);

        INSERT INTO
          department_type_stage (tenant_id, department_id)
        SELECT DISTINCT
          tenant_id,
          department_type_department_id
        FROM
          user_dimension_ledger_stage
        WHERE
          tenant_id IS NOT NULL AND
          department_type_department_id IS NOT NULL
        ;

        VACUUM FULL ANALYZE department_type_stage;

        BEGIN;
        LOCK TABLE department_type IN EXCLUSIVE MODE;

        INSERT INTO
          department_type (tenant_id, department_id)
        SELECT
          department_type_stage.tenant_id,
          department_type_stage.department_id
        FROM
          department_type_stage
        LEFT OUTER JOIN
          department_type
        ON
          department_type.tenant_id = department_type_stage.tenant_id AND
          department_type.department_id = department_type_stage.department_id
        WHERE
          department_type.tenant_id IS NULL AND
          department_type.department_id IS NULL
        ;

        VACUUM FULL ANALYZE department_type;

        COMMIT;
      EOS
    end
  end

  context 'for postgres dimension with file missing references' do
    let(:source) { catalog.postgres.misc_file.stage_table(table: target) }

    subject(:result) { transform.insert_reference_values(source, target).to_s }

    it 'should not render insert_reference_values template' do
      is_expected.to eq("\n")
    end
  end
end
