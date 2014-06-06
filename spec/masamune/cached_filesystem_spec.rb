require 'spec_helper'

describe Masamune::CachedFilesystem do
  let(:filesystem) { Masamune::MockFilesystem.new }
  let(:cached_filesystem) { Masamune::CachedFilesystem.new(filesystem) }

  context 'when path is present' do
    before do
      filesystem.touch!('/a/b/c/1.txt', '/a/b/c/2.txt', '/a/b/c/3.txt')
      filesystem.should_receive(:stat).with('/a/b/c/*').once.and_call_original
    end

    it 'calls Filesystem#stat once for multiple calls' do
      cached_filesystem.exists?('/a/b/c/1.txt').should == true
      cached_filesystem.exists?('/a/b/c/2.txt').should == true
      cached_filesystem.exists?('/a/b/c/3.txt').should == true
      cached_filesystem.exists?('/a/b/c/4.txt').should == false
      cached_filesystem.exists?('/a').should == true
      cached_filesystem.exists?('/a/b').should == true
      cached_filesystem.exists?('/a/b/c').should == true
      cached_filesystem.glob('/a/*').should_not be_empty
      cached_filesystem.glob('/a/b/*').should_not be_empty
      cached_filesystem.glob('/a/b/c/*').should_not be_empty
      cached_filesystem.glob('/a/b/c/*.txt').should_not be_empty
    end
  end

  context 'when path is present, checking for similar non existant paths' do
    before do
      filesystem.touch!('/y=2013/m=1/d=22/00000')
      filesystem.should_receive(:stat).with('/y=2013/m=1/d=22/*').once.and_call_original
    end

    it 'calls Filesystem#stat once for multiple calls' do
      cached_filesystem.exists?('/y=2013/m=1/d=22/00000').should == true
      cached_filesystem.exists?('/y=2013/m=1/d=22').should == true
      cached_filesystem.exists?('/y=2013/m=1/d=2').should == false
      cached_filesystem.glob('/y=2013/*').should_not be_empty
      cached_filesystem.glob('/y=2013/m=1/*').should_not be_empty
      cached_filesystem.glob('/y=2013/m=1/d=22/*').should_not be_empty
    end
  end

  context 'when path is present, checking for similar existing paths' do
    before do
      filesystem.touch!('/logs/box1_123.txt', '/logs/box2_123.txt', '/logs/box3_123.txt')
      filesystem.should_receive(:stat).with('/logs/*').once.and_call_original
    end

    it 'calls Filesystem#stat once for multiple calls' do
      cached_filesystem.exists?('/logs/box1_123.txt').should == true
      cached_filesystem.exists?('/logs/box1_456.txt').should == false
      cached_filesystem.exists?('/logs/box2_123.txt').should == true
      cached_filesystem.exists?('/logs/box2_456.txt').should == false
      cached_filesystem.exists?('/logs/box3_123.txt').should == true
      cached_filesystem.exists?('/logs/box3_456.txt').should == false
      cached_filesystem.exists?('/logs/box4_123.txt').should == false
      cached_filesystem.exists?('/logs/box4_456.txt').should == false
      cached_filesystem.glob('/logs/*').should_not be_empty
      cached_filesystem.glob('/logs/*.txt').should_not be_empty
      cached_filesystem.glob('/logs/box1_*.txt').should_not be_empty
      cached_filesystem.glob('/logs/box2_*.txt').should_not be_empty
      cached_filesystem.glob('/logs/box3_*.txt').should_not be_empty
      cached_filesystem.glob('/logs/box*.txt').should have(3).items
      cached_filesystem.glob('/logs/box*.csv').should be_empty
    end
  end

  context 'when path is missing' do
    before do
      filesystem.touch!('/a/b/c')
      filesystem.should_receive(:stat).with('/a/b/c/*').once.and_call_original
      filesystem.should_receive(:stat).with('/a/b/*').once.and_call_original
    end

    it 'calls Filesystem#stat once for multiple calls' do
      cached_filesystem.exists?('/a/b/c/1.txt').should == false
      cached_filesystem.exists?('/a/b/c/2.txt').should == false
      cached_filesystem.exists?('/a/b/c/3.txt').should == false
      cached_filesystem.exists?('/a').should == true
      cached_filesystem.exists?('/a/b').should == true
      cached_filesystem.exists?('/a/b/c').should == true
      cached_filesystem.glob('/a/*').should_not be_empty
      cached_filesystem.glob('/a/b/*').should_not be_empty
      cached_filesystem.glob('/a/b/c/*').should be_empty
      cached_filesystem.glob('/a/b/c/*.txt').should be_empty
    end
  end
end
