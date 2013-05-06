require 'spec_helper'

describe Masamune::Filesystem do
  let(:instance) { Masamune::Filesystem.new }

  describe '#[]' do
    before do
      instance.add_location(:home_dir, '/home')
    end
    it { instance[:home_dir].should == '/home' }
  end
end
