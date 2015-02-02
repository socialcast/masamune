require 'spec_helper'

describe Masamune::Actions::Transform do
  let(:environment) { double }
  let(:registry) { Masamune::Schema::Registry.new(environment) }

  before do
    registry.schema :postgres do
      dimension 'user', type: :four do
        column 'tenant_id', type: :integer, index: true
        column 'user_id',   type: :integer, index: true, surrogate_key: true
      end

      file 'user', format: :csv, headers: true do
        column 'id', type: :integer
        column 'tenant_id', type: :integer
        column 'updated_at', type: :timestamp
      end

      map from: postgres.user_file, to: postgres.user_dimension do
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

  let(:source_file) { Tempfile.new('masamune') }

  let(:klass) do
    Class.new do
      include Masamune::HasEnvironment
      include Masamune::Actions::Transform
    end
  end

  let(:instance) { klass.new }
  let(:postgres) { registry.postgres }

  before do
    instance.environment = MasamuneExampleGroup
  end

  describe '.load_dimension' do
    before do
      mock_command(/\Apsql/, mock_success)
    end

    subject { instance.load_dimension(source_file, postgres.user_file, postgres.user_dimension) }

    it { is_expected.to be_success }
  end

  describe '.load_fact' do
    let(:date) { DateTime.civil(2014, 8) }

    before do
      mock_command(/\Apsql/, mock_success)
    end

    subject { instance.load_fact(source_file, postgres.visits_file, postgres.visits_fact, date) }

    it { is_expected.to be_success }
  end

  describe '.relabel_dimension' do
    before do
      mock_command(/\Apsql/, mock_success)
    end

    subject { instance.relabel_dimension(postgres.user_dimension) }

    it { is_expected.to be_success }
  end

  describe '.consolidate_dimension' do
    before do
      mock_command(/\Apsql/, mock_success)
    end

    subject { instance.consolidate_dimension(postgres.user_dimension) }

    it { is_expected.to be_success }
  end
end
