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

describe Masamune::Transform::DeduplicateDimension do
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
    subject(:result) { transform.deduplicate_dimension(target.stage_table(suffix: 'consolidated'), target.stage_table(suffix: 'deduplicated')).to_s }

    it 'should render deduplicate_dimension template' do
      is_expected.to eq <<-EOS.strip_heredoc
        WITH consolidated AS (
          SELECT
            user_account_state_type_id,
            tenant_id,
            user_id,
            preferences,
            start_at,
            date_trunc('hour',start_at) AS dimension_grain
          FROM
            user_consolidated_dimension_stage
        )
        INSERT INTO
          user_deduplicated_dimension_stage (user_account_state_type_id, tenant_id, user_id, preferences, start_at)
        SELECT DISTINCT
          user_account_state_type_id,
          tenant_id,
          user_id,
          preferences,
          start_at
        FROM (
          SELECT
            user_account_state_type_id,
            tenant_id,
            user_id,
            preferences,
            dimension_grain AS start_at,
            CASE
            WHEN (LAG(user_account_state_type_id) OVER w = user_account_state_type_id) AND (LAG(tenant_id) OVER w = tenant_id) AND (LAG(user_id) OVER w = user_id) AND (LAG(dimension_grain) OVER w = dimension_grain) THEN
              1
            ELSE
              0
            END AS duplicate
          FROM
            consolidated
          WINDOW w AS (PARTITION BY tenant_id, user_id ORDER BY start_at DESC)
        ) tmp
        WHERE
          duplicate = 0
        ;
      EOS
    end
  end
end
