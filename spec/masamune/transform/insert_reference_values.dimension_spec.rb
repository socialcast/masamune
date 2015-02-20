require 'spec_helper'

describe Masamune::Transform::InsertReferenceValues do
  before do
    catalog.schema :postgres do
      dimension 'department', type: :mini do
        column 'uuid', type: :uuid, surrogate_key: true
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
