require 'spec_helper'

describe 'Masamune::Transform::DefineTable with Masamune::Schema::Fact' do
  let(:transform) { Object.new.extend(Masamune::Transform::DefineTable) }

  subject { transform.define_table(fact).to_s }

  let(:dimension) do
    Masamune::Schema::Dimension.new id: 'user', type: :two,
      columns: [
        Masamune::Schema::Column.new(id: 'tenant_id', index: true),
        Masamune::Schema::Column.new(id: 'user_id', index: true)
      ]
  end

  let(:fact) do
    Masamune::Schema::Fact.new id: 'visits', partition: 'y%Ym%m',
      references: [Masamune::Schema::TableReference.new(dimension)],
      columns: [
        Masamune::Schema::Column.new(id: 'total', type: :integer)
      ]
  end

  it 'should render table template' do
    is_expected.to eq <<-EOS.strip_heredoc
      CREATE TABLE IF NOT EXISTS visits_fact
      (
        user_dimension_uuid UUID NOT NULL REFERENCES user_dimension(uuid),
        total INTEGER NOT NULL,
        time_key INTEGER NOT NULL,
        last_modified_at TIMESTAMP NOT NULL DEFAULT NOW()
      );

      DO $$ BEGIN
      IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'visits_fact_user_dimension_uuid_index') THEN
      CREATE INDEX visits_fact_user_dimension_uuid_index ON visits_fact (user_dimension_uuid);
      END IF; END $$;

      DO $$ BEGIN
      IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'visits_fact_time_key_index') THEN
      CREATE INDEX visits_fact_time_key_index ON visits_fact (time_key);
      END IF; END $$;
    EOS
  end

  let(:environment) { double }
  let(:registry) { Masamune::Schema::Registry.new(environment) }

  before do
    registry.schema :postgres do
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

  let(:data) { (1..3).map { |i| double(path: "output_#{i}.csv") } }
  let(:target) { registry.postgres.visits_fact }
  let(:source) { registry.postgres.visits_file }

  describe '#define_table with data files' do
    subject(:result) { transform.define_table(source.as_table(target), data).to_s }

    it 'should eq render table template' do
      is_expected.to eq <<-EOS.strip_heredoc
        CREATE TEMPORARY TABLE IF NOT EXISTS visits_fact_file
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

        COPY visits_fact_file FROM 'output_1.csv' WITH (FORMAT 'csv');
        COPY visits_fact_file FROM 'output_2.csv' WITH (FORMAT 'csv');
        COPY visits_fact_file FROM 'output_3.csv' WITH (FORMAT 'csv');

        CREATE INDEX visits_fact_file_date_dimension_date_id_index ON visits_fact_file (date_dimension_date_id);
        CREATE INDEX visits_fact_file_tenant_dimension_tenant_id_index ON visits_fact_file (tenant_dimension_tenant_id);
        CREATE INDEX visits_fact_file_user_dimension_user_id_index ON visits_fact_file (user_dimension_user_id);
        CREATE INDEX visits_fact_file_user_agent_type_name_index ON visits_fact_file (user_agent_type_name);
        CREATE INDEX visits_fact_file_user_agent_type_version_index ON visits_fact_file (user_agent_type_version);
        CREATE INDEX visits_fact_file_feature_type_name_index ON visits_fact_file (feature_type_name);
        CREATE INDEX visits_fact_file_time_key_index ON visits_fact_file (time_key);
      EOS
    end
  end
end
