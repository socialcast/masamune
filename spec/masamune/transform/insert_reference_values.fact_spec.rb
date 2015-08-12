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
      dimension 'date', type: :one do
        column 'date_id', type: :integer, unique: true, index: true, natural_key: true
      end

      dimension 'user_agent', type: :mini do
        column 'name', type: :string, unique: true, index: 'shared'
        column 'version', type: :string, unique: true, index: 'shared', default: 'Unknown'
        column 'description', type: :string, null: true, ignore: true
      end

      dimension 'feature', type: :mini do
        column 'name', type: :string, unique: true, index: true
      end

      dimension 'tenant', type: :two do
        column 'tenant_id', type: :integer, index: true, natural_key: true
      end

      dimension 'user', type: :two do
        column 'tenant_id', type: :integer, index: true, natural_key: true
        column 'user_id', type: :integer, index: true, natural_key: true
      end

      fact 'visits', partition: 'y%Ym%m' do
        references :date
        references :tenant
        references :user
        references :user_agent, insert: true
        references :feature, insert: true
        measure 'total', type: :integer
      end

      file 'visits' do
        column 'date.date_id', type: :integer
        column 'tenant.tenant_id', type: :integer
        column 'user.user_id', type: :integer
        column 'user_agent.name', type: :string
        column 'user_agent.version', type: :string
        column 'feature.name', type: :string
        column 'time_key', type: :integer
        column 'total', type: :integer
      end
    end
  end

  let(:target) { catalog.postgres.visits_fact }
  let(:source) { catalog.postgres.visits_file.stage_table(table: target) }

  context 'with postgres fact' do
    subject(:result) { transform.insert_reference_values(source, target).to_s }

    it 'should eq render insert_reference_values template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TEMPORARY TABLE IF NOT EXISTS user_agent_type_stage (LIKE user_agent_type INCLUDING ALL);

        INSERT INTO
          user_agent_type_stage (name, version)
        SELECT DISTINCT
          user_agent_type_name,
          COALESCE(user_agent_type_version, 'Unknown')
        FROM
          visits_fact_stage
        WHERE
          user_agent_type_name IS NOT NULL
        ;

        ANALYZE user_agent_type_stage;

        BEGIN;
        LOCK TABLE user_agent_type IN EXCLUSIVE MODE;

        INSERT INTO
          user_agent_type (name, version)
        SELECT
          user_agent_type_stage.name,
          user_agent_type_stage.version
        FROM
          user_agent_type_stage
        LEFT OUTER JOIN
          user_agent_type
        ON
          user_agent_type.name = user_agent_type_stage.name AND
          user_agent_type.version = user_agent_type_stage.version
        WHERE
          user_agent_type.name IS NULL AND
          user_agent_type.version IS NULL
        ;

        ANALYZE user_agent_type;

        COMMIT;

        CREATE TEMPORARY TABLE IF NOT EXISTS feature_type_stage (LIKE feature_type INCLUDING ALL);

        INSERT INTO
          feature_type_stage (name)
        SELECT DISTINCT
          feature_type_name
        FROM
          visits_fact_stage
        WHERE
          feature_type_name IS NOT NULL
        ;

        ANALYZE feature_type_stage;

        BEGIN;
        LOCK TABLE feature_type IN EXCLUSIVE MODE;

        INSERT INTO
          feature_type (name)
        SELECT
          feature_type_stage.name
        FROM
          feature_type_stage
        LEFT OUTER JOIN
          feature_type
        ON
          feature_type.name = feature_type_stage.name
        WHERE
          feature_type.name IS NULL
        ;

        ANALYZE feature_type;

        COMMIT;
      EOS
    end
  end
end
