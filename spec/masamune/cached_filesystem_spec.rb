require 'spec_helper'

describe Masamune::CachedFilesystem do
  let(:filesystem) { MockFilesystem.new }
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
