require 'spec_helper'

describe Masamune::Actions::Transform do
  let(:source_file) { Tempfile.new('masamune').path }

  let(:user_file) do
    Masamune::Schema::File.new id: 'user',
      columns: [
        Masamune::Schema::Column.new(id: 'id', type: :integer),
        Masamune::Schema::Column.new(id: 'tenant_id', type: :integer),
        Masamune::Schema::Column.new(id: 'updated_at', type: :timestamp),
      ]
  end

  let(:user_dimension) do
    Masamune::Schema::Dimension.new id: 'user', type: :four,
      columns: [
        Masamune::Schema::Column.new(id: 'tenant_id', index: true, surrogate_key: true),
        Masamune::Schema::Column.new(id: 'user_id', index: true, surrogate_key: true)
      ]
  end

  let(:map) do
    Masamune::Schema::Map.new(
      fields: {
        'tenant_id'                => 'tenant_id',
        'user_id'                  => 'id',
        'start_at'                 => 'updated_at',
        'source_kind'              => 'users',
        'delta'                    => 1})
  end

  let(:visit_fact) do
    Masamune::Schema::Fact.new id: 'visits', partition: 'y%Ym%m',
      references: [user_dimension],
      columns: [
        Masamune::Schema::Column.new(id: 'total', type: :integer)
      ]
  end

  let(:klass) do
    Class.new do
      include Masamune::HasEnvironment
      include Masamune::Actions::Transform
    end
  end

  let(:instance) { klass.new }

  before do
    instance.environment = MasamuneExampleGroup
  end

  describe '.load_dimension' do
    before do
      mock_command(/\Apsql/, mock_success)
    end

    subject { instance.load_dimension(source_file, user_file, user_dimension, map) }

    it { is_expected.to be_success }
  end

  describe '.load_fact' do
    let(:date) { DateTime.civil(2014, 8) }
    let(:data) { Masamune::DataPlanSet.new(source_file) }

    before do
      mock_command(/\Apsql/, mock_success)
    end

    subject { instance.load_fact(data, visit_fact.as_file, visit_fact, date) }

    it { is_expected.to be_success }
  end
end
