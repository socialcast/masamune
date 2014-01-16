require 'spec_helper'

describe Masamune::Template do
  describe '.render_to_file' do

    context 'with simple template' do
      let(:template) { File.expand_path('../../fixtures/simple.sql.erb', __FILE__) }

      subject do
        File.read(described_class.render_to_file(template, table: 'zombo'))
      end

      it { should == "SELECT * FROM zombo;\n" }
    end

    context 'with aggregate template' do
      let(:template) { File.expand_path('../../fixtures/aggregate.sql.erb', __FILE__) }

      subject do
        File.read(described_class.render_to_file(template))
      end

      it do should == <<-EOS.gsub(/^\s*/,'')
        SHOW TABLES;
        SELECT * FROM foo;
        SELECT * FROM bar;
      EOS
      end
    end
  end
end
