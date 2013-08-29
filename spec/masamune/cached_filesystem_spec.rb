require 'spec_helper'

describe Masamune::CachedFilesystem do
  let(:filesystem) { Masamune::MockFilesystem.new }
  let(:cached_filesystem) { Masamune::CachedFilesystem.new(filesystem) }

  context 'when path is present' do
    before do
      filesystem.touch!('/a/b/c/1.txt', '/a/b/c/2.txt', '/a/b/c/3.txt')
      filesystem.should_receive(:glob).with('/a/b/c/*').once.and_call_original
    end

    it 'calls Filesystem#glob once for multiple calls' do
      cached_filesystem.exists?('/a/b/c/1.txt').should be_true
      cached_filesystem.exists?('/a/b/c/2.txt').should be_true
      cached_filesystem.exists?('/a/b/c/3.txt').should be_true
      cached_filesystem.exists?('/a/b/c/4.txt').should be_false
      cached_filesystem.exists?('/a').should be_true
      cached_filesystem.exists?('/a/b').should be_true
      cached_filesystem.exists?('/a/b/c').should be_true
    end
  end

  context 'when path is present with file glob' do
    before do
      filesystem.touch!('/logs/box1_123.txt', '/logs/box2_123.txt', '/logs/box3_123.txt')
      filesystem.should_receive(:glob).with('/logs/*').once.and_call_original
    end

    it 'calls Filesystem#glob once for multiple calls' do
      cached_filesystem.glob('/logs/box1_*.txt').should_not be_empty
      cached_filesystem.glob('/logs/box2_*.txt').should_not be_empty
      cached_filesystem.glob('/logs/box3_*.txt').should_not be_empty
      cached_filesystem.exists?('/logs/box1_123.txt').should be_true
      cached_filesystem.exists?('/logs/box1_456.txt').should be_false
      cached_filesystem.exists?('/logs/box2_123.txt').should be_true
      cached_filesystem.exists?('/logs/box2_456.txt').should be_false
      cached_filesystem.exists?('/logs/box3_123.txt').should be_true
      cached_filesystem.exists?('/logs/box3_456.txt').should be_false
      cached_filesystem.exists?('/logs/box4_123.txt').should be_false
      cached_filesystem.exists?('/logs/box4_456.txt').should be_false
    end
  end

  context 'when path is missing' do
    before do
      filesystem.touch!('/a/b/c')
      filesystem.should_receive(:glob).with('/a/b/c/*').once.and_call_original
    end

    it 'calls Filesystem#glob once for multiple calls' do
      cached_filesystem.exists?('/a/b/c/1.txt').should be_false
      cached_filesystem.exists?('/a/b/c/2.txt').should be_false
      cached_filesystem.exists?('/a/b/c/3.txt').should be_false
      cached_filesystem.exists?('/a').should be_true
      cached_filesystem.exists?('/a/b').should be_true
      cached_filesystem.exists?('/a/b/c').should be_true
    end
  end
end
