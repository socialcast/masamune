require 'spec_helper'

describe Masamune::Template do
  describe '.generate' do
    let(:template) { File.expand_path('../../fixtures/basic.sql.erb', __FILE__) }

    subject do
      File.read(described_class.generate(template, table: 'zombo'))
    end

    it { should == "SELECT * FROM zombo;\n" }
  end
end
