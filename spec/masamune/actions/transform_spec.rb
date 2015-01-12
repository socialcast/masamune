require 'spec_helper'

describe Masamune::Actions::Transform do
  let(:environment) { double }
  let(:registry) { Masamune::Schema::Registry.new(environment) }

  before do
    registry.schema do
      dimension 'user', type: :four do
        column 'tenant_id', type: :integer, index: true
        column 'user_id',   type: :integer, index: true, surrogate_key: true
      end

      file 'user', format: :csv, headers: true do
        column 'id', type: :integer
        column 'tenant_id', type: :integer
        column 'updated_at', type: :timestamp
      end

      map from: files[:user], to: dimensions[:user] do
        field 'user_id', 'id'
        field 'tenant_id'
        field 'source_kind', 'users'
        field 'start_at', 'updated_at'
        field 'delta', 1
      end

      fact 'visits', partition: 'y%Ym%m' do
        references :user
        measure 'total', type: :integer
      end

      file 'visits' do
        column 'user.tenant_id', type: :integer
        column 'user.user_id', type: :integer
        column 'time_key', type: :integer
        column 'total', type: :integer
      end
    end
  end

  let(:source_file) { Tempfile.new('masamune').path }

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

    subject { instance.load_dimension(source_file, registry.files[:user], registry.dimensions[:user]) }

    it { is_expected.to be_success }
  end

  describe '.load_fact' do
    let(:date) { DateTime.civil(2014, 8) }
    let(:data) { Masamune::DataPlan::Set.new(source_file) }

    before do
      mock_command(/\Apsql/, mock_success)
    end

    subject { instance.load_fact(data, registry.files[:visits], registry.facts[:visits], date) }

    it { is_expected.to be_success }
  end
end
