require 'spec_helper'
require 'active_support/core_ext/string/strip'

describe Masamune::Schema::Map do
  let(:environment) { double }
  let(:registry) { Masamune::Schema::Registry.new(environment) }

  before do
    registry.schema do
      dimension 'user_account_state', type: :mini do
        column 'name', type: :string, unique: true
        column 'description', type: :string, null: true
      end

      dimension 'user', type: :four do
        references :user_account_state
        column 'cluster_id', index: true, surrogate_key: true
        column 'tenant_id', index: true, surrogate_key: true
        column 'user_id', index: true, surrogate_key: true
        column 'preferences', type: :key_value, null: true
        column 'admin', type: :boolean
        column 'source', type: :string
      end

      file 'user', format: :csv do
        column 'id', type: :integer
        column 'tenant_id', type: :integer
        column 'admin', type: :boolean
        column 'preferences', type: :yaml
        column 'deleted_at', type: :timestamp
      end

      map from: files[:user], to: dimensions[:user], headers: true do
        field 'tenant_id', 'tenant_id'
        field 'user_id', 'id'
        field 'user_account_state.name' do |row|
          row[:deleted_at] ? 'deleted' : 'active'
        end
        field 'admin' do |row|
          row[:admin]
        end
        field 'preferences', 'preferences'
        field 'source', 'users_file'
        field 'cluster_id', 100
      end
    end
  end

  context 'without from' do
    subject(:map) { described_class.new }
    it { expect { map }.to raise_error ArgumentError }
  end

  context 'without to' do
    subject(:map) { described_class.new(from: registry.files[:user]) }
    it { expect { map }.to raise_error ArgumentError }
  end

  describe '#apply' do
    let(:source) do
      registry.files[:user]
    end

    let(:map) do
      registry.files[:user].map(to: registry.dimensions[:user])
    end

    let(:target) do
      registry.dimensions[:user].as_file(map.columns)
    end

    let(:source_data) do
      <<-EOS.strip_heredoc
        id,tenant_id,junk_id,deleted_at,admin,preferences
        1,30,X,,0,,
        2,40,Y,2014-02-26 18:15:51 UTC,1,"---
        :enabled: true
        "
      EOS
    end

    let(:target_data) do
      <<-EOS.strip_heredoc
        tenant_id,user_id,user_account_state_type_name,admin,preferences,source,cluster_id
        30,1,active,FALSE,{},users_file,100
        40,2,deleted,TRUE,"{""enabled"":true}",users_file,100
      EOS
    end

    let(:input) { StringIO.new(source_data) }
    let(:output) { StringIO.new }

    before do
      map.apply(source.bind(input), target.bind(output))
    end

    subject { output.string }

    it 'should match target data' do
      is_expected.to eq(target_data)
    end
  end
end
