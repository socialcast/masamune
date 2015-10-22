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

describe Masamune::Template do
  describe '.render_to_file' do
    let(:parameters) { {} }

    subject do
      File.read(described_class.render_to_file(template, parameters))
    end

    context 'with invalid file' do
      let(:template) { 'not_a_file.txt' }
      it { expect { subject }.to raise_error IOError }
    end

    context 'with invalid template' do
      let(:template) { File.expand_path('../../fixtures/invalid.sql.erb', __FILE__) }
      it { expect { subject }.to raise_error IOError, /not_found.sql.erb/ }
    end

    context 'with simple template' do
      let(:template) { File.expand_path('../../fixtures/simple.sql.erb', __FILE__) }
      let(:parameters) { {table: 'zombo'} }

      it { is_expected.to eq("SELECT * FROM zombo;\n") }
    end

    context 'with template with comments' do
      let(:template) { File.expand_path('../../fixtures/comment.sql.erb', __FILE__) }
      it { is_expected.to eq("SELECT 1;\n") }
    end

    context 'with template with unnecessary whitespace' do
      let(:template) { File.expand_path('../../fixtures/whitespace.sql.erb', __FILE__) }

      it { is_expected.to eq("SELECT 1;\n\nSELECT 2;\n") }
    end

    context 'with aggregate template' do
      let(:template) { File.expand_path('../../fixtures/aggregate.sql.erb', __FILE__) }

      it do is_expected.to eq <<-EOS.gsub(/^\s*/,'')
        SHOW TABLES;
        SELECT * FROM foo;
        SELECT * FROM bar;
      EOS
      end
    end

    context 'with aggregate template with relative path' do
      let(:template) { File.join(File.dirname(__FILE__), '..', 'fixtures', 'relative.sql.erb') }
      it { is_expected.to eq("SELECT * FROM relative;\n") }
    end

    context 'with packaged template' do
      let(:template) { 'hive/define_schema.hql.erb' }
      it { is_expected.to_not be_nil }
    end
  end
end
