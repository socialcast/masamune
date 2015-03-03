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
  end
end
