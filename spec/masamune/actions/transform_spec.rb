require 'spec_helper'

describe Masamune::Actions::Transform do
  let(:environment) { double }
  let(:catalog) { Masamune::Schema::Catalog.new(environment) }

  before do
    catalog.schema :postgres do
      dimension 'date', type: :date do
        column 'date_id', type: :integer, unique: true, index: true, natural_key: true
        column 'date_epoch', type: :integer
        column 'month_epoch', type: :integer
        column 'year_epoch', type: :integer
      end

      dimension 'user', type: :four do
        column 'tenant_id', type: :integer, index: true
        column 'user_id',   type: :integer, index: true, surrogate_key: true
      end

      file 'user' do
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

      fact 'visits', partition: 'y%Ym%m', grain: %w(hourly daily monthly) do
        references :date
        references :user

        measure 'total', type: :integer
      end

      file 'visits_hourly' do
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
  let(:postgres) { catalog.postgres }

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

  describe '.load_fact' do
    let(:date) { DateTime.civil(2014, 8) }

    before do
      mock_command(/\Apsql/, mock_success)
    end

    subject { instance.load_fact(source_file, postgres.visits_hourly_file, postgres.visits_hourly_fact, date) }

    it { is_expected.to be_success }
  end

  describe '.rollup_fact' do
    let(:date) { DateTime.civil(2014, 8) }

    before do
      mock_command(/\Apsql/, mock_success)
    end

    subject { instance.rollup_fact(postgres.visits_hourly_fact, postgres.visits_daily_fact, date) }

    it { is_expected.to be_success }
  end
end
