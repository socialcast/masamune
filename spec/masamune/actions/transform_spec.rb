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
  let(:options) { { extra_args: true } }

  before do
    expect(instance).to receive(:postgres).with(hash_including(extra_args: true)).and_call_original
  end

  describe '.load_dimension' do
    subject { instance.load_dimension(source_file, postgres.user_file, postgres.user_dimension, options) }

    context 'without :map' do
      before do
        expect_any_instance_of(Masamune::Schema::Map).to_not receive(:apply)
        mock_command(/\APGOPTIONS=.* psql/, mock_success)
      end

      it { is_expected.to be_success }
    end

    context 'with :map' do
      before do
        catalog.schema :postgres do
          map from: postgres.user_file, to: postgres.user_dimension do |row|
            {
              user_id: row[:id],
              tenant_id: row[:tenant_id],
              source_kind: 'users',
              start_at: row[:updated_at],
              delta: 1
            }
          end
        end

        expect_any_instance_of(Masamune::Schema::Map).to receive(:apply).and_call_original
        mock_command(/\APGOPTIONS=.* psql/, mock_success)
      end

      it { is_expected.to be_success }
    end
  end

  describe '.relabel_dimension' do
    before do
      mock_command(/\APGOPTIONS=.* psql/, mock_success)
    end

    subject { instance.relabel_dimension(postgres.user_dimension, options) }

    it { is_expected.to be_success }
  end

  describe '.consolidate_dimension' do
    before do
      mock_command(/\APGOPTIONS=.* psql/, mock_success)
    end

    subject { instance.consolidate_dimension(postgres.user_dimension, options) }

    it { is_expected.to be_success }
  end

  describe '.load_fact' do
    let(:date) { DateTime.civil(2014, 8) }

    context 'without :map' do
      before do
        expect_any_instance_of(Masamune::Schema::Map).to_not receive(:apply)
        mock_command(/\APGOPTIONS=.* psql/, mock_success)
      end

      subject { instance.load_fact(source_file, postgres.visits_hourly_file, postgres.visits_hourly_fact, date, options) }

      it { is_expected.to be_success }
    end

    context 'with :map' do
      before do
        catalog.schema :postgres do
          map from: postgres.visits_hourly_file, to: postgres.visits_hourly_fact, distinct: true
        end

        expect_any_instance_of(Masamune::Schema::Map).to receive(:apply).and_call_original
        mock_command(/\APGOPTIONS=.* psql/, mock_success)
      end

      subject { instance.load_fact(source_file, postgres.visits_hourly_file, postgres.visits_hourly_fact, date, options) }

      it { is_expected.to be_success }
    end
  end

  describe '.rollup_fact' do
    let(:date) { DateTime.civil(2014, 8) }

    before do
      mock_command(/\APGOPTIONS=.* psql/, mock_success)
    end

    subject { instance.rollup_fact(postgres.visits_hourly_fact, postgres.visits_daily_fact, date, options) }

    it { is_expected.to be_success }
  end
end
