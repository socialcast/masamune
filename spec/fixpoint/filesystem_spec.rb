require 'spec_helper'

describe Fixpoint::Filesystem do
  let(:configuration) { Fixpoint::Configuration.new }
  let(:instance) { Fixpoint::Filesystem.new(configuration) }

  describe '#[]' do
    before do
      instance.add_location(:home_dir, '/home')
    end
    it { instance[:home_dir].should == '/home' }
  end
end
