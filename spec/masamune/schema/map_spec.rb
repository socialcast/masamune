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
        references :user_account_state, label: :hr
        column 'cluster_id', index: true, surrogate_key: true
        column 'tenant_id', index: true, surrogate_key: true
        column 'user_id', index: true, surrogate_key: true
        column 'preferences', type: :key_value, null: true
        column 'admin', type: :boolean
        column 'source', type: :string
      end

      file 'user', format: :csv, headers: true do
        column 'id', type: :integer
        column 'tenant_id', type: :integer
        column 'admin', type: :boolean
        column 'preferences', type: :yaml
        column 'deleted_at', type: :timestamp
      end

      map from: files[:user], to: dimensions[:user] do
        field 'tenant_id', 'tenant_id'
        field 'user_id', 'id'
        field 'user_account_state.name' do |row|
          row[:deleted_at] ? 'deleted' : 'active'
        end
        field 'hr_user_account_state.name' do |row|
          row[:deleted_at] ? 'deleted' : 'active'
        end
        field 'admin' do |row|
          row[:admin]
        end
        field 'preferences_now', 'preferences'
        field 'source', 'users_file'
        field 'cluster_id', 100
      end

      event 'user' do
        attribute 'id', type: :integer, immutable: true
        attribute 'tenant_id', type: :integer, immutable: true
        attribute 'admin', type: :boolean
        attribute 'preferences', type: :json
      end

      map from: events[:user], to: dimensions[:user] do
        field 'tenant_id'
        field 'user_id', 'id'
        field 'user_account_state.name' do |row|
          row[:type] =~ /delete/ ? 'deleted' : 'active'
        end
        field 'admin' do |row|
          row[:type] =~ /delete/ ? row[:admin_was] : row[:admin_now]
        end
        field 'preferences_now', 'preferences_now'
        field 'preferences_was', 'preferences_was'
        field 'source', 'user_event'
        field 'cluster_id', 100
      end

      map from: events[:user], to: files[:user] do
        field 'id'
        field 'tenant_id'
        field 'deleted_at' do |row|
          row[:created_at] if row[:type] =~ /delete/
        end
        field 'admin', 'admin_now'
        field 'preferences', 'preferences_now'
      end
    end
  end

  context 'without source' do
    subject(:map) { described_class.new }
    it { expect { map }.to raise_error ArgumentError }
  end

  context 'without target' do
    subject(:map) { described_class.new(source: registry.files[:user]) }
    it { expect { map }.to raise_error ArgumentError }
  end

  describe '#apply' do
    let(:map) do
      source.map(to: target)
    end

    let(:input) { StringIO.new(source_data) }
    let(:output) { StringIO.new }

    before do
      map.apply(input, output)
    end

    subject { output.string }

    context 'from csv file to dimension' do
      let(:source) do
        registry.files[:user]
      end

      let(:target) do
        registry.dimensions[:user]
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
          tenant_id,user_id,user_account_state_type_name,hr_user_account_state_type_name,admin,preferences_now,source,cluster_id
          30,1,active,active,FALSE,{},users_file,100
          40,2,deleted,deleted,TRUE,"{""enabled"":true}",users_file,100
        EOS
      end

      it 'should match target data' do
        is_expected.to eq(target_data)
      end
    end

    context 'from event to dimension' do
      let(:source) do
        registry.events[:user]
      end

      let(:target) do
        registry.dimensions[:user]
      end

      let(:source_data) do
        <<-EOS.strip_heredoc
          X	user_create	1	30	0	\\N	\\N	\\N
          Y	user_delete	2	40	0	1	"{""enabled"":true}"	\\N
        EOS
      end

      let(:target_data) do
        <<-EOS.strip_heredoc
          tenant_id,user_id,user_account_state_type_name,admin,preferences_now,preferences_was,source,cluster_id
          30,1,active,FALSE,{},{},user_event,100
          40,2,deleted,TRUE,"{""enabled"":true}",{},user_event,100
        EOS
      end

      it 'should match target data' do
        is_expected.to eq(target_data)
      end
    end

    context 'from event to tsv file' do
      let(:source) do
        registry.events[:user]
      end

      let(:target) do
        registry.files[:user].tap do |file|
          file.format = :tsv
          file.headers = true
          file.columns[:preferences].type = :json
        end
      end

      let(:source_data) do
        <<-EOS.strip_heredoc
          X	user_create	1	30	0	\\N	\\N	\\N	0	\\N
          Y	user_delete	2	40	0	1	"{""enabled"":true}"	\\N	0	2014-02-26T18:15:51Z
        EOS
      end

      let(:target_data) do
        <<-EOS.strip_heredoc
          id	tenant_id	deleted_at	admin	preferences
          1	30		FALSE	{}
          2	40	2014-02-26T18:15:51Z	FALSE	"{""enabled"":true}"
        EOS
      end


      it 'should match target data' do
        is_expected.to eq(target_data)
      end
    end

    context 'from event to csv file' do
      let(:source) do
        registry.events[:user]
      end

      let(:target) do
        registry.files[:user]
      end

      let(:source_data) do
        <<-EOS.strip_heredoc
          X	user_create	1	30	0	\\N	\\N	\\N	0	\\N
          Y	user_delete	2	40	0	1	"{""enabled"":true}"	\\N	0	2014-02-26T18:15:51Z
        EOS
      end

      let(:target_data) do
        <<-EOS.strip_heredoc
          id,tenant_id,deleted_at,admin,preferences
          1,30,,FALSE,"--- {}
          "
          2,40,2014-02-26T18:15:51Z,FALSE,"---
          enabled: true
          "
        EOS
      end

      it 'should match target data' do
        is_expected.to eq(target_data)
      end
    end
  end
end
