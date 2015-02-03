require 'spec_helper'

describe Masamune::Transform::RelabelDimension do
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

  context 'for postgres dimension' do
    subject(:result) { transform.relabel_dimension(target).to_s }

    it 'should eq render relabel_dimension template' do
      is_expected.to eq <<-EOS.strip_heredoc
        BEGIN;
        LOCK TABLE user_dimension IN EXCLUSIVE MODE;

        UPDATE user_dimension SET version = NULL;

        UPDATE
          user_dimension
        SET
          version = tmp.version
        FROM
          (
            SELECT
              uuid,
              tenant_id,
              user_id,
              start_at,
              rank() OVER (PARTITION BY tenant_id, user_id ORDER BY start_at) AS version
            FROM
              user_dimension
            GROUP BY
              uuid, tenant_id, user_id, start_at
           ) AS tmp
        WHERE
          user_dimension.uuid = tmp.uuid
        ;

        UPDATE user_dimension SET end_at = NULL;

        UPDATE
          user_dimension
        SET
          end_at = tmp.end_at
        FROM
          (
            SELECT
              uuid,
              start_at,
              tenant_id,
              user_id,
              LEAD(start_at, 1) OVER (PARTITION BY tenant_id, user_id ORDER BY start_at) AS end_at
            FROM
              user_dimension
            GROUP BY
              uuid, tenant_id, user_id, start_at
           ) AS tmp
        WHERE
          user_dimension.uuid = tmp.uuid
        ;

        COMMIT;
      EOS
    end
  end
end
