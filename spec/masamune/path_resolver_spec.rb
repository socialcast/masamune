require 'spec_helper'

describe Masamune::PathResolver do
  let(:instance) { Masamune::PathResolver.new }

  describe '#[]' do
    before do
      instance.add_path(:home_dir, '/home')
    end
    it { instance[:home_dir].should == '/home' }
  end

  describe '#type' do
    subject { instance.type(path) }

    context 'file:// prefix' do
      let(:path) { 'file:///tmp' }
      it { should == :hdfs }
    end

    context 's3:// prefix' do
      let(:path) { 's3://bucket/tmp' }
      it { should == :s3 }
    end

    context 's3n:// prefix' do
      let(:path) { 's3n://bucket/tmp' }
      it { should == :s3 }
    end

    context '/ prefix' do
      let(:path) { '/tmp' }
      it { should == :local }
    end

    context 'no prefix' do
      let(:path) { 'tmp' }
      it { should == :local }
    end
  end
end
