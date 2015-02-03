require 'spec_helper'

describe Masamune::Transform::StageDimension do
  let(:transform) { Object.new.extend(described_class) }
  let(:environment) { double }
  let(:registry) { Masamune::Schema::Registry.new(environment) }

  before do
    registry.schema :postgres do
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
        column 'uuid', type: :uuid, surrogate_key: true
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

      file 'user', headers: true do
        column 'tenant_id', type: :integer
        column 'user_id', type: :integer
        column 'department.department_id', type: :integer
        column 'user_account_state.name', type: :string
        column 'hr_user_account_state.name', type: :string
        column 'preferences_now', type: :json
        column 'start_at', type: :timestamp
        column 'source_kind', type: :string
        column 'delta', type: :integer
      end
    end
  end

  let(:target) { registry.postgres.user_dimension.ledger_table }
  let(:source) { registry.postgres.user_file.as_table(target) }

  context 'for postgres dimension' do
    subject(:result) { transform.stage_dimension(source, target).to_s }

    it 'should eq render stage_dimension template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TEMPORARY TABLE IF NOT EXISTS user_dimension_ledger_stage (LIKE user_dimension_ledger INCLUDING ALL);

        INSERT INTO
          user_dimension_ledger_stage (department_type_uuid, user_account_state_type_id, hr_user_account_state_type_id, tenant_id, user_id, preferences_now, source_kind, start_at, delta)
        SELECT
          department_type.uuid,
          user_account_state_type.id,
          hr_user_account_state_type.id,
          user_dimension_ledger_file.tenant_id,
          user_dimension_ledger_file.user_id,
          json_to_hstore(user_dimension_ledger_file.preferences_now),
          user_dimension_ledger_file.source_kind,
          user_dimension_ledger_file.start_at,
          user_dimension_ledger_file.delta
        FROM
          user_dimension_ledger_file
        LEFT JOIN
          department_type AS department_type
        ON
          department_type.department_id = user_dimension_ledger_file.department_type_department_id AND
          department_type.tenant_id = user_dimension_ledger_file.tenant_id
        LEFT JOIN
          user_account_state_type AS user_account_state_type
        ON
          user_account_state_type.name = user_dimension_ledger_file.user_account_state_type_name
        LEFT JOIN
          user_account_state_type AS hr_user_account_state_type
        ON
          hr_user_account_state_type.name = user_dimension_ledger_file.hr_user_account_state_type_name
        ;
      EOS
    end
  end
end
