require 'spec_helper'
require 'active_support/core_ext/string/strip'

describe Masamune::Schema::Map do
  let(:source_data) do
    <<-EOS.strip_heredoc
      id,tenant_id,junk_id,deleted_at,admin,preferences
      1,30,X,,0,,
      2,40,Y,2014-02-26 18:15:51 UTC,1,"---
      :enabled: true
      "
    EOS
  end

  let(:source) do
    Masamune::Schema::File.new name: 'source_user', buffer: StringIO.new(source_data),
      format: :csv,
      columns: [
        Masamune::Schema::Column.new(name: 'id', type: :integer),
        Masamune::Schema::Column.new(name: 'tenant_id', type: :integer),
        Masamune::Schema::Column.new(name: 'admin', type: :boolean),
        Masamune::Schema::Column.new(name: 'preferences', type: :yaml),
        Masamune::Schema::Column.new(name: 'deleted_at', type: :timestamp)
      ]
  end

  let(:mini_dimension) do
    Masamune::Schema::Dimension.new name: 'user_account_state', type: :mini,
      columns: [
        Masamune::Schema::Column.new(name: 'name', type: :string, unique: true),
        Masamune::Schema::Column.new(name: 'description', type: :string)
      ]
  end

  let(:dimension) do
    Masamune::Schema::Dimension.new name: 'user', references: [mini_dimension],
      columns: [
        Masamune::Schema::Column.new(name: 'cluster_id', type: :integer),
        Masamune::Schema::Column.new(name: 'user_id', type: :integer),
        Masamune::Schema::Column.new(name: 'tenant_id', type: :integer),
        Masamune::Schema::Column.new(name: 'preferences', type: :key_value),
        Masamune::Schema::Column.new(name: 'admin', type: :boolean),
        Masamune::Schema::Column.new(name: 'source', type: :string)
      ]
  end

  context 'with source file and target file' do
    let(:map) do
      described_class.new(
        headers: true,
        fields: {
          'tenant_id'               => 'tenant_id',
          'user_id'                 => 'id',
          'user_account_state.name' => ->(row) { row[:deleted_at] ? 'deleted' : 'active' },
          'admin'                   => ->(row) { row[:admin] },
          'preferences'             => 'preferences',
          'source'                  => 'users_file',
          'cluster_id'              => 100 })
    end

    let(:target) do
      dimension.as_file(map.columns)
    end

    let(:target_data) do
      <<-EOS.strip_heredoc
        tenant_id,user_id,user_account_state_type_name,admin,preferences,source,cluster_id
        30,1,active,FALSE,{},users_file,100
        40,2,deleted,TRUE,"{""enabled"":true}",users_file,100
      EOS
    end

    before do
      map.apply(source, target)
    end

    subject { target.buffer.string }

    it 'should match target data' do
      is_expected.to eq(target_data)
    end
  end
end
