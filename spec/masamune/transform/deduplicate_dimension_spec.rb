require 'spec_helper'

describe Masamune::Transform::DeduplicateDimension do
  let(:environment) { double }
  let(:registry) { Masamune::Schema::Registry.new(environment) }

  before do
    registry.schema do
      dimension 'user_account_state', type: :mini do
        column 'name', type: :string, unique: true
        column 'description', type: :string
      end

      dimension 'user', type: :four do
        references :user_account_state
        column 'tenant_id', index: true, natural_key: true
        column 'user_id', index: true, natural_key: true
        column 'preferences', type: :key_value, null: true
      end
    end
  end

  let(:target) { registry.dimensions[:user] }
  let(:transform) { Object.new.extend(described_class) }

  describe '#deduplicate_dimension' do
    subject(:result) { transform.deduplicate_dimension(target.stage_table('consolidated'), target.stage_table('deduplicated')).to_s }

    it 'should render deduplicate_dimension template' do
      is_expected.to eq <<-EOS.strip_heredoc
        INSERT INTO
          user_deduplicated_dimension_stage (user_account_state_type_id, tenant_id, user_id, preferences, parent_uuid, record_uuid, start_at)
        SELECT DISTINCT
          user_account_state_type_id,
          tenant_id,
          user_id,
          preferences,
          parent_uuid,
          record_uuid,
          start_at
        FROM (
          SELECT
            user_account_state_type_id,
            tenant_id,
            user_id,
            preferences,
            parent_uuid,
            record_uuid,
            start_at,
            CASE
            WHEN (LAG(user_account_state_type_id) OVER w = user_account_state_type_id) AND (LAG(tenant_id) OVER w = tenant_id) AND (LAG(user_id) OVER w = user_id) AND (LAG(preferences) OVER w = preferences) THEN
              1
            ELSE
              0
            END AS duplicate
          FROM
            user_consolidated_dimension_stage
          WINDOW w AS (PARTITION BY tenant_id, user_id ORDER BY start_at)
        ) tmp
        WHERE
          duplicate = 0
        ;
      EOS
    end
  end
end
