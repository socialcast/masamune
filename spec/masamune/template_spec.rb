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

    context 'with simple template' do
      let(:template) { File.expand_path('../../fixtures/simple.sql.erb', __FILE__) }
      let(:parameters) { {table: 'zombo'} }

      it { should == "SELECT * FROM zombo;\n" }
    end

    context 'with aggregate template' do
      let(:template) { File.expand_path('../../fixtures/aggregate.sql.erb', __FILE__) }

      it do should == <<-EOS.gsub(/^\s*/,'')
        SHOW TABLES;
        SELECT * FROM foo;
        SELECT * FROM bar;
      EOS
      end
    end
  end
end
