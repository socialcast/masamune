#  The MIT License (MIT)
#
#  Copyright (c) 2014-2016, VMware, Inc. All Rights Reserved.
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

describe Masamune::Transform::ConsolidateDimension do
  before do
    catalog.schema :postgres do
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

  let(:target) { catalog.postgres.user_dimension }

  context 'with postgres dimension' do
    subject(:result) { transform.consolidate_dimension(target).to_s }

    it 'should render combined template' do
      is_expected.to eq Masamune::Template.combine \
        transform.define_table(target.stage_table(suffix: 'consolidated_forward')),
        transform.define_table(target.stage_table(suffix: 'consolidated_reverse')),
        transform.define_table(target.stage_table(suffix: 'consolidated')),
        transform.define_table(target.stage_table(suffix: 'deduplicated')),
        transform.snapshot_dimension(target.ledger_table, target.stage_table(suffix: 'consolidated_forward'), 'ASC'),
        transform.snapshot_dimension(target.ledger_table, target.stage_table(suffix: 'consolidated_reverse'), 'DESC'),
        transform.bulk_upsert(target.stage_table(suffix: 'consolidated_reverse'), target.stage_table(suffix: 'consolidated')),
        transform.bulk_upsert(target.stage_table(suffix: 'consolidated_forward'), target.stage_table(suffix: 'consolidated')),
        transform.deduplicate_dimension(target.stage_table(suffix: 'consolidated'), target.stage_table(suffix: 'deduplicated')),
        transform.bulk_upsert(target.stage_table(suffix: 'deduplicated'), target),
        transform.relabel_dimension(target)
    end
  end
end
