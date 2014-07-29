require 'spec_helper'
require 'active_support/core_ext/string/strip'

describe Masamune::Schema::CSVFile do
  let(:environment) { double }
  let(:filesystem) { Masamune::MockFilesystem.new }
  let(:file) { 'users_1.csv' }
  let(:columns) {
    [
      Masamune::Schema::Column.new(name: 'id', type: :integer),
      Masamune::Schema::Column.new(name: 'tenant_id', type: :integer),
      Masamune::Schema::Column.new(name: 'active', type: :string, transform: ->(record) { record[:deleted_at] ? 'deleted' : 'active' })
    ]
  }

  let(:instance) { described_class.new(environment, name: 'user', file: file, columns: columns) }

  before do
    allow(instance).to receive(:filesystem) { filesystem }
  end

  describe '#transform' do
    let(:csv_data) do
      <<-EOS.strip_heredoc
      id,tenant_id,junk_id,deleted_at
      1,1,X,
      2,1,Y,2014-02-26 18:15:51 UTC
      EOS
    end

    before do
      filesystem.write(csv_data, 'users_1.csv')
    end

    subject(:result) { instance.transform }

    let(:result_data) { File.read(result) }

    it do
      expect(result).to be_a(Tempfile)
      expect(result_data).to eq <<-EOS.strip_heredoc
      1,1,active
      2,1,deleted
      EOS
    end
  end

  describe '#as_table' do
    before do
      allow(instance).to receive(:transform) { double(path: 'output.csv') }
    end

    subject(:result) { instance.as_table }

    it 'render sql to load csv file' do
      expect(result).to eq <<-EOS.strip_heredoc
        CREATE TEMPORARY TABLE IF NOT EXISTS user_stage
        (
          id INTEGER NOT NULL,
          tenant_id INTEGER NOT NULL,
          active VARCHAR NOT NULL
        );

        COPY user_stage FROM 'output.csv' WITH (FORMAT 'csv');
      EOS
    end
  end
end
