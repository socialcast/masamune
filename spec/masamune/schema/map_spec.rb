#  The MIT License (MIT)
#
#  Copyright (c) 2014-2015, VMware, Inc. All Rights Reserved.
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

require 'spec_helper'

describe Masamune::Schema::Map do
  let(:environment) { double(logger: double) }
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
        column 'deleted_at', type: :timestamp, null: true
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

    context 'with undefined function' do
      let(:source) { catalog.postgres.user_file }
      let(:target) { catalog.postgres.user_dimension }
      let(:source_data) { '' }
      let(:target_data) { '' }

      before do
        catalog.schema :hive do
          map from: postgres.user_file, to: postgres.user_dimension do |row|
          end
        end
      end

      it { expect { subject }.to raise_error ArgumentError, /function for map between .* does not return output for default input/ }
    end

    context 'from csv file to postgres dimension' do
      before do
        catalog.schema :files do
          map from: postgres.user_file, to: postgres.user_dimension, distinct: true do |row|
            {
              'tenant_id'                  => row[:tenant_id],
              'user_id'                    => row[:id],
              'user_account_state.name'    => row[:deleted_at] ? 'deleted' :  'active',
              'hr_user_account_state.name' => row[:deleted_at] ? 'deleted' :  'active',
              'admin'                      => row[:admin],
              'preferences'                => row[:preferences],
              'source'                     => 'users_file',
              'cluster_id'                 => 100
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
          # NOTE intentional duplicate record
          1,30,X,,0,,
          2,40,Y,2014-02-26 18:15:51 UTC,1,"---
          :enabled: true
          "
          # NOTE record is intentionally invalid
          ,50,X,,0,
        EOS
      end

      let(:target_data) do
        <<-EOS.strip_heredoc
          tenant_id,user_id,user_account_state_type_name,hr_user_account_state_type_name,admin,preferences,source,cluster_id
          30,1,active,active,FALSE,{},users_file,100
          40,2,deleted,deleted,TRUE,"{""enabled"":true}",users_file,100
        EOS
      end

      before do
        expect(environment.logger).to receive(:warn).with(/row .* missing required columns 'user_id'/)
      end

      it 'should match target data' do
        is_expected.to eq(target_data)
      end

      it_behaves_like 'apply input/output'
    end

    context 'from tsv file to postgres dimension' do
      before do
        catalog.schema :files do
          file 'input', format: :tsv, headers: false do
            column 'id', type: :integer
            column 'tenant_id', type: :integer
            column 'admin', type: :boolean
            column 'preferences', type: :json
            column 'deleted_at', type: :timestamp, null: true
          end

          map from: files.input, to: postgres.user_dimension do |row|
            raise if row[:tenant_id] == 42
            {
              'tenant_id'               => row[:tenant_id],
              'user_id'                 => row[:id],
              'user_account_state.name' => row[:deleted_at] ? 'deleted' : 'active',
              'admin'                   => row[:admin],
              'preferences'             => row[:preferences],
              'source'                  => 'user_file',
              'cluster_id'              => 100
            }
          end
        end
      end

      let(:source) do
        catalog.files.input
      end

      let(:target) do
        catalog.postgres.user_dimension
      end

      before do
        expect(environment.logger).to receive(:warn).with(/failed to process '{.*}' for #{target.name}/).ordered
        expect(environment.logger).to receive(:warn).with(/failed to parse '{.*}' for #{source.name}/).ordered
      end

      let(:target_data) do
        <<-EOS.strip_heredoc
          tenant_id,user_id,user_account_state_type_name,admin,preferences,source,cluster_id
          30,1,active,FALSE,{},user_file,100
          30,1,active,FALSE,{},user_file,100
          40,2,deleted,TRUE,"{""enabled"":true}",user_file,100
        EOS
      end

      context 'with quoted json' do
        let(:source_data) do
          <<-EOS.strip_heredoc
            1	30	0			
            # NOTE intentional duplicate record
            1	30	0			
            1	42	0		
            2	40	1	"{""enabled"":true}"	2015-07-19 00:00:00
            # NOTE record is intentionally invalid
            3	50	0	INVALID_JSON	
          EOS
        end

        it 'should match target data' do
          is_expected.to eq(target_data)
        end

        it_behaves_like 'apply input/output'
      end

      context 'with raw json' do
        let(:source_data) do
          <<-EOS.strip_heredoc
            1	30	0			
            # NOTE intentional duplicate record
            1	30	0			
            1	42	0		
            2	40	1	{"enabled":true}	2015-07-19 00:00:00
            # NOTE record is intentionally invalid
            3	50	0	INVALID_JSON	
          EOS
        end

        it 'should match target data' do
          is_expected.to eq(target_data)
        end

        it_behaves_like 'apply input/output'
      end
    end

    context 'from tsv file to csv file' do
      before do
        catalog.schema :files do
          file 'input', format: :tsv, headers: false do
            column 'id', type: :integer
            column 'tenant_id', type: :integer
            column 'admin', type: :boolean
            column 'preferences', type: :json
            column 'deleted_at', type: :timestamp, null: true
          end

          file 'output', format: :csv, headers: true do
            column 'id', type: :integer
            column 'tenant_id', type: :integer
            column 'admin', type: :boolean
            column 'preferences', type: :yaml
            column 'deleted_at', type: :timestamp, null: true
          end

          map from: files.input, to: files.output do |row|
            {
              'id'          => row[:id],
              'tenant_id'   => row[:tenant_id],
              'deleted_at'  => row[:deleted_at],
              'admin'       => row[:admin],
              'preferences' => row[:preferences]
            }
          end
        end
      end

      let(:source) do
        catalog.files.input
      end

      let(:target) do
        catalog.files.output
      end

      let(:source_data) do
        <<-EOS.strip_heredoc
          1	30	0		
          2	40	0	"{""enabled"":true}"	2014-02-26T18:15:51.000Z
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

    context 'with multiple outputs' do
      before do
        catalog.schema :files do
          file 'input' do
            column 'id', type: :integer
          end

          file 'output' do
            column 'id', type: :integer
          end

          map from: files.input, to: files.output do |row|
            [row, row]
          end
        end
      end

      let(:source) do
        catalog.files.input
      end

      let(:target) do
        catalog.files.output
      end

      let(:source_data) do
        <<-EOS.strip_heredoc
          1
          2
        EOS
      end

      let(:target_data) do
        <<-EOS.strip_heredoc
          1
          1
          2
          2
        EOS
      end

      it 'should match target data' do
        is_expected.to eq(target_data)
      end

      it_behaves_like 'apply input/output'
    end
  end

  describe Masamune::Schema::Map::JSONEncoder do
    let(:io) { StringIO.new }
    let(:store) { double(json_encoding: :raw, format: :csv) }
    let(:encoder) { described_class.new(io, store) }

    subject { encoder.gets }

    context 'with raw empty json' do
      before do
        io.write '{},{}'
        io.rewind
      end
      it { is_expected.to eq(%Q{"{}","{}"}) }
    end

    context 'with raw quoted json' do
      before do
        io.write '"{}","{}"'
        io.rewind
      end
      it { is_expected.to eq(%Q{"{}","{}"}) }
    end

    context 'with raw json' do
      before do
        io.write '{"enabled":true,"state":""}'
        io.rewind
      end
      it { is_expected.to eq(%Q{"{""enabled"":true,""state"":""""}"}) }
    end

    context 'with quoted json' do
      before do
        io.write '"{""enabled"":true,""state"":""""}"'
        io.rewind
      end
      it { is_expected.to eq(%Q{"{""enabled"":true,""state"":""""}"}) }
    end
  end
end
