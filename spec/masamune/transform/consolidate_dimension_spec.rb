require 'spec_helper'

describe Masamune::Transform::ConsolidateDimension do
  before do
    registry.schema :postgres do
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

  let(:target) { registry.postgres.user_dimension }

  context 'with postgres dimension' do
    subject(:result) { transform.consolidate_dimension(target).to_s }

    it 'should render combined template' do
      is_expected.to eq Masamune::Template.combine \
        transform.define_table(target.stage_table('consolidated_forward')),
        transform.define_table(target.stage_table('consolidated_reverse')),
        transform.define_table(target.stage_table('consolidated')),
        transform.define_table(target.stage_table('deduplicated')),
        transform.snapshot_dimension(target.ledger_table, target.stage_table('consolidated_forward'), 'ASC'),
        transform.snapshot_dimension(target.ledger_table, target.stage_table('consolidated_reverse'), 'DESC'),
        transform.bulk_upsert(target.stage_table('consolidated_forward'), target.stage_table('consolidated')),
        transform.bulk_upsert(target.stage_table('consolidated_reverse'), target.stage_table('consolidated')),
        transform.deduplicate_dimension(target.stage_table('consolidated'), target.stage_table('deduplicated')),
        transform.bulk_upsert(target.stage_table('deduplicated'), target),
        transform.relabel_dimension(target)
    end
  end
end
