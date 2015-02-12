require 'spec_helper'

describe Masamune::Transform::LoadFact do
  before do
    catalog.schema :postgres do
      dimension 'date', type: :date do
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

  let(:files) { (1..3).map { |i| double(path: "output_#{i}.csv") } }
  let(:date) { DateTime.civil(2014,8) }
  let(:target) { catalog.postgres.visits_fact }
  let(:source) { catalog.postgres.visits_file }
  let(:source_table) { source.as_table(target) }

  context 'for postgres fact' do
    subject(:result) { transform.load_fact(files, source, target, date).to_s }

    it 'should render combined template' do
      is_expected.to eq Masamune::Template.combine \
        transform.define_table(source_table, files),
        transform.insert_reference_values(source_table, target),
        transform.stage_fact(source_table, target, date)
    end
  end
end
