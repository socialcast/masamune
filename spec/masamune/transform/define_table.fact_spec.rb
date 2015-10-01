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

describe Masamune::Transform::DefineTable do
  before do
    catalog.schema :postgres do
      dimension 'cluster', type: :mini do
        column 'id', type: :sequence, surrogate_key: true, auto: true
        column 'name', type: :string

        row name: 'current_database()', attributes: {default: true}
      end

      dimension 'date', type: :date do
        column 'date_id', type: :integer, natural_key: true
      end

      dimension 'user_agent', type: :mini do
        references :cluster

        column 'name', type: :string, unique: true, index: 'shared'
        column 'version', type: :string, unique: true, index: 'shared', default: 'Unknown'
        column 'description', type: :string, null: true, ignore: true
      end

      dimension 'feature', type: :mini do
        column 'name', type: :string, unique: true, index: true
      end

      dimension 'tenant', type: :two do
        references :cluster

        column 'tenant_id', type: :integer, natural_key: true
      end

      dimension 'user', type: :two do
        references :cluster

        column 'tenant_id', type: :integer, natural_key: true
        column 'user_id', type: :integer, natural_key: true
      end

      dimension 'group', type: :two do
        references :cluster

        column 'group_id', type: :integer, natural_key: true
      end

      fact 'visits', partition: 'y%Ym%m' do
        partition :y
        partition :m
        references :cluster
        references :date
        references :tenant
        references :user
        references :group, multiple: true
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

    catalog.schema :hive do
      dimension 'date', type: :date, implicit: true do
        column 'date_id', type: :integer, natural_key: true
      end

      dimension 'user', type: :two, implicit: true do
        column 'user_id', type: :integer, natural_key: true
      end

      dimension 'group', type: :two, implicit: true do
        column 'group_id', type: :integer, natural_key: true
      end

      dimension 'user_agent', type: :mini do
        column 'name', type: :string
        column 'version', type: :string
        column 'description', type: :string, ignore: true
      end

      fact 'visits', grain: :hourly do
        partition :y
        partition :m
        partition :d
        references :date
        references :user
        references :group, multiple: true
        references :user_agent, denormalize: true
        measure 'total'
      end
    end
  end

  context 'for postgres fact' do
    let(:target) { catalog.postgres.visits_fact }

    subject(:result) { transform.define_table(target).to_s }

    it 'should eq render table template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TABLE IF NOT EXISTS visits_fact
        (
          cluster_type_id INTEGER NOT NULL DEFAULT default_cluster_type_id(),
          date_dimension_id INTEGER NOT NULL,
          tenant_dimension_id INTEGER NOT NULL,
          user_dimension_id INTEGER NOT NULL,
          group_dimension_id INTEGER[] NOT NULL,
          user_agent_type_id INTEGER NOT NULL,
          feature_type_id INTEGER NOT NULL,
          total INTEGER NOT NULL,
          time_key INTEGER NOT NULL,
          last_modified_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
      EOS
    end
  end

  context 'for postgres fact partition with :post' do
    let(:target) { catalog.postgres.visits_fact.partition_table(Date.civil(2015, 01, 01)) }

    subject(:result) { transform.define_table(target, [], :post).to_s }

    it 'should eq render table template' do
      is_expected.to match /ALTER TABLE visits_fact_y2015m01 INHERIT visits_fact;/
      is_expected.to match /ALTER TABLE visits_fact_y2015m01 ADD CONSTRAINT visits_fact_y2015m01_time_key_check CHECK \(time_key >= 1420070400 AND time_key < 1422748800\);/
    end
  end

  describe 'for fact table from file with sources files' do
    let(:files) { (1..3).map { |i| double(path: "output_#{i}.csv") } }
    let(:target) { catalog.postgres.visits_fact }
    let(:source) { catalog.postgres.visits_file }

    subject(:result) { transform.define_table(source.stage_table(suffix: 'file', table: target, inherit: false), files).to_s }

    it 'should eq render table template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TEMPORARY TABLE IF NOT EXISTS visits_file_fact_stage
        (
          date_dimension_date_id INTEGER,
          tenant_dimension_tenant_id INTEGER,
          user_dimension_user_id INTEGER,
          user_agent_type_name VARCHAR,
          user_agent_type_version VARCHAR,
          feature_type_name VARCHAR,
          time_key INTEGER,
          total INTEGER
        );

        COPY visits_file_fact_stage FROM 'output_1.csv' WITH (FORMAT 'csv', HEADER true);
        COPY visits_file_fact_stage FROM 'output_2.csv' WITH (FORMAT 'csv', HEADER true);
        COPY visits_file_fact_stage FROM 'output_3.csv' WITH (FORMAT 'csv', HEADER true);

        CREATE INDEX visits_file_fact_stage_964dac1_index ON visits_file_fact_stage (date_dimension_date_id);
        CREATE INDEX visits_file_fact_stage_5a187ed_index ON visits_file_fact_stage (feature_type_name);
        CREATE INDEX visits_file_fact_stage_90fc13c_index ON visits_file_fact_stage (tenant_dimension_tenant_id);
        CREATE INDEX visits_file_fact_stage_6444ed3_index ON visits_file_fact_stage (time_key);
        CREATE INDEX visits_file_fact_stage_99c433b_index ON visits_file_fact_stage (user_agent_type_name);
        CREATE INDEX visits_file_fact_stage_d5d236f_index ON visits_file_fact_stage (user_agent_type_version);
        CREATE INDEX visits_file_fact_stage_30f3cca_index ON visits_file_fact_stage (user_dimension_user_id);
        CREATE INDEX visits_file_fact_stage_8608ecc_index ON visits_file_fact_stage (date_dimension_date_id, time_key);
        CREATE INDEX visits_file_fact_stage_28291db_index ON visits_file_fact_stage (feature_type_name, time_key);
        CREATE INDEX visits_file_fact_stage_69e4501_index ON visits_file_fact_stage (tenant_dimension_tenant_id, time_key);
        CREATE INDEX visits_file_fact_stage_766cbfa_index ON visits_file_fact_stage (user_agent_type_name, time_key);
        CREATE INDEX visits_file_fact_stage_0fe2101_index ON visits_file_fact_stage (user_agent_type_version, time_key);
        CREATE INDEX visits_file_fact_stage_b0abfed_index ON visits_file_fact_stage (user_dimension_user_id, time_key);

        VACUUM FULL ANALYZE visits_file_fact_stage;
      EOS
    end

    context 'with file' do
      subject(:result) { transform.define_table(source.stage_table(table: target), files.first).to_s }
      it 'should eq render table template' do
        is_expected.to_not be_nil
      end
    end

    context 'with Set' do
      subject(:result) { transform.define_table(source.stage_table(table: target), Set.new(files)).to_s }
      it 'should eq render table template' do
        is_expected.to_not be_nil
      end
    end
  end

  context 'for postgres fact with degenerate reference' do
    before do
      catalog.clear!
      catalog.schema :postgres do
        fact 'visits' do
          references :message_kind, degenerate: true
          measure 'count', aggregate: :sum
        end
      end
    end

    let(:target) { catalog.postgres.visits_fact }

    subject(:result) { transform.define_table(target).to_s }

    it 'should eq render table template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TABLE IF NOT EXISTS visits_fact
        (
          message_kind_type_id INTEGER,
          count INTEGER NOT NULL,
          time_key INTEGER NOT NULL,
          last_modified_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
      EOS
    end
  end

  context 'for hive fact' do
    let(:target) { catalog.hive.visits_hourly_fact }

    subject(:result) { transform.define_table(target).to_s }

    it 'should eq render table template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TABLE IF NOT EXISTS visits_hourly_fact
        (
          date_dimension_date_id INT,
          user_dimension_user_id INT,
          group_dimension_group_id ARRAY<INT>,
          user_agent_type_name STRING,
          user_agent_type_version STRING,
          total INT,
          time_key INT
        )
        PARTITIONED BY (y INT, m INT, d INT)
        TBLPROPERTIES ('serialization.null.format' = '');
      EOS
    end
  end
end
