require 'spec_helper'

describe Masamune::Schema::Map do
  let(:environment) { double }
  let(:catalog) { Masamune::Schema::Catalog.new(environment) }

  before do
    catalog.schema :postgres do
      dimension 'user_account_state', type: :mini do
        column 'name', type: :string, unique: true
        column 'description', type: :string, null: true
      end

      dimension 'user', type: :four do
        references :user_account_state
        references :user_account_state, label: :hr
        column 'cluster_id', index: true, natural_key: true
        column 'tenant_id', index: true, natural_key: true
        column 'user_id', index: true, natural_key: true
        column 'preferences', type: :key_value, null: true
        column 'admin', type: :boolean
        column 'source', type: :string
      end

      file 'user' do
        column 'id', type: :integer
        column 'tenant_id', type: :integer
        column 'admin', type: :boolean
        column 'preferences', type: :yaml
        column 'deleted_at', type: :timestamp
      end
    end

    catalog.schema :hive do
      event 'user' do
        attribute 'id', type: :integer, immutable: true
        attribute 'tenant_id', type: :integer, immutable: true
        attribute 'admin', type: :boolean
        attribute 'preferences', type: :json
      end

      dimension 'tenant', type: :two, implicit: true do
        column 'tenant_id'
      end

      fact 'user' do
        references :tenant
        measure 'delta'
      end

      file 'user' do
        column 'id', type: :integer
        column 'tenant_id', type: :integer
        column 'admin', type: :boolean
        column 'preferences', type: :json
        column 'deleted_at', type: :timestamp
      end
    end
  end

  context 'without source' do
    subject(:map) { described_class.new }
    it { expect { map }.to raise_error ArgumentError }
  end

  context 'without target' do
    subject(:map) { described_class.new(source: catalog.postgres.user_file) }
    it { expect { map }.to raise_error ArgumentError }
  end

  let(:input) { Tempfile.new('masamune') }
  let(:output) { Tempfile.new('masamune') }

  describe '#apply' do
    let(:map) do
      source.map(to: target)
    end

    before do
      output.truncate(0)
      output.rewind
      input.truncate(0)
      input.write(source_data)
      input.close
    end

    subject do
      map.apply(input, output)
      output.readlines.join
    end

    shared_examples_for 'apply input/output' do
      context 'with IO' do
        subject do
          io = File.open(output, 'a+')
          map.apply(File.open(input), io)
          io.rewind
          io.readlines.join
        end
        it 'should match target data' do
          is_expected.to eq(target_data)
        end
      end

      context 'with String' do
        subject do
          map.apply(input.path, output.path)
          File.readlines(output.path).join
        end
        it 'should match target data' do
          is_expected.to eq(target_data)
        end
      end
    end

    context 'from csv file to dimension' do
      before do
        catalog.schema :files do
          map from: postgres.user_file, to: postgres.user_dimension do |row|
            {
              'tenant_id':                   row[:tenant_id],
              'user_id':                     row[:id],
              'user_account_state.name':     row[:deleted_at] ? 'deleted' :  'active',
              'hr_user_account_state.name':  row[:deleted_at] ? 'deleted' :  'active',
              'admin':                       row[:admin],
              'preferences_now':             row[:preferences],
              'source':                      'users_file',
              'cluster_id':                  100
            }
          end
        end
      end

      let(:source) do
        catalog.postgres.user_file
      end

      let(:target) do
        catalog.postgres.user_dimension
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

      it_behaves_like 'apply input/output'
    end

    context 'from event to postgres dimension' do
      before do
        catalog.schema :files do
          map from: hive.user_event, to: postgres.user_dimension do |row|
            {
              'tenant_id':                   row[:tenant_id],
              'user_id':                     row[:id],
              'user_account_state.name':     row[:type] =~ /delete/ ? 'deleted' : 'active',
              'admin':                       row[:type] =~ /delete/ ? row[:admin_was] : row[:admin_now],
              'preferences_now':             row[:preferences_now],
              'preferences_was':             row[:preferences_was],
              'source':                      'user_event',
              'cluster_id':                  100
            }
          end
        end
      end

      let(:source) do
        catalog.hive.user_event
      end

      let(:target) do
        catalog.postgres.user_dimension
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

      it_behaves_like 'apply input/output'
    end

    context 'from event to tsv file' do
      before do
        catalog.schema :files do
          map from: hive.user_event, to: hive.user_file do |row|
            {
              'id':           row[:id],
              'tenant_id':    row[:tenant_id],
              'deleted_at':   row[:type] =~ /delete/ ? row[:created_at] : nil,
              'admin':        row[:admin_now],
              'preferences':  row[:preferences_now]
            }
          end
        end
      end

      let(:source) do
        catalog.hive.user_event
      end

      let(:target) do
        catalog.hive.user_file
      end

      let(:source_data) do
        <<-EOS.strip_heredoc
          X	user_create	1	30	0	\\N	\\N	\\N	0	\\N
          Y	user_delete	2	40	0	1	"{""enabled"":true}"	\\N	0	2014-02-26T18:15:51Z
        EOS
      end

      let(:target_data) do
        <<-EOS.strip_heredoc
          1	30		FALSE	{}
          2	40	2014-02-26T18:15:51.000Z	FALSE	"{""enabled"":true}"
        EOS
      end


      it 'should match target data' do
        is_expected.to eq(target_data)
      end

      it_behaves_like 'apply input/output'
    end

    context 'from event to csv file' do
      before do
        catalog.schema :files do
          map from: hive.user_event, to: postgres.user_file do |row|
            {
              'id':           row[:id],
              'tenant_id':    row[:tenant_id],
              'deleted_at':   row[:type] =~ /delete/ ? row[:created_at] : nil,
              'admin':        row[:admin_now],
              'preferences':  row[:preferences_now]
            }
          end
        end
      end

      let(:source) do
        catalog.hive.user_event
      end

      let(:target) do
        catalog.postgres.user_file
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
          2,40,2014-02-26T18:15:51.000Z,FALSE,"---
          enabled: true
          "
        EOS
      end

      it 'should match target data' do
        is_expected.to eq(target_data)
      end

      it_behaves_like 'apply input/output'
    end

    context 'from event to fact' do
      before do
        catalog.schema :files do
          map from: hive.user_event, to: hive.user_fact do |row|
            if row[:type] =~ /update/
              [
                {
                  'tenant.tenant_id':  row[:tenant_id],
                  'delta':             0,
                  'time_key':          row[:created_at]
                },
                {
                  'tenant.tenant_id':  row[:tenant_id],
                  'delta':             0,
                  'time_key':          row[:created_at]
                }
              ]
            else
              {
                'tenant.tenant_id':  row[:tenant_id],
                'delta':             row[:type] =~ /create/ ? 1 :  -1,
                'time_key':          row[:created_at]
              }
            end
          end
        end
      end

      let(:source) do
        catalog.hive.user_event
      end

      let(:target) do
        catalog.hive.user_fact
      end

      let(:source_data) do
        <<-EOS.strip_heredoc
          X	user_create	3	10	0	1	"{""enabled"":true}"	\\N	\\N	2015-01-01T00:10:00Z
          Y	user_update	3	10	0	1	"{""enabled"":true}"	\\N	\\N	2015-01-01T00:20:00Z
          Z	user_delete	3	10	0	1	"{""enabled"":true}"	\\N	\\N	2015-01-01T00:30:00Z
        EOS
      end

      let(:target_data) do
        <<-EOS.strip_heredoc
          10	1	1420071000
          10	0	1420071600
          10	0	1420071600
          10	-1	1420072200
        EOS
      end

      it 'should match target data' do
        is_expected.to eq(target_data)
      end

      it_behaves_like 'apply input/output'
    end
  end
end
