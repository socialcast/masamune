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

describe Masamune::Transform::RelabelDimension do
  before do
    allow_any_instance_of(Masamune::Schema::Table).to receive(:lock_id).and_return(42)

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

  context 'for postgres dimension' do
    subject(:result) { transform.relabel_dimension(target).to_s }

    it 'should eq render relabel_dimension template' do
      is_expected.to eq <<-EOS.strip_heredoc
        SELECT pg_advisory_lock(42);

        BEGIN;

        UPDATE
          user_dimension
        SET
          version = tmp.version
        FROM
          (
            SELECT
              id,
              tenant_id,
              user_id,
              start_at,
              rank() OVER (PARTITION BY tenant_id, user_id ORDER BY start_at) AS version
            FROM
              user_dimension
           ) AS tmp
        WHERE
          user_dimension.id = tmp.id
        ;

        UPDATE
          user_dimension
        SET
          end_at = tmp.end_at
        FROM
          (
            SELECT
              id,
              start_at,
              tenant_id,
              user_id,
              LEAD(start_at, 1) OVER (PARTITION BY tenant_id, user_id ORDER BY start_at) AS end_at
            FROM
              user_dimension
           ) AS tmp
        WHERE
          user_dimension.id = tmp.id
        ;

        COMMIT;

        SELECT pg_advisory_unlock(42);
      EOS
    end
  end
end
