require 'spec_helper'

describe Masamune::CachedFilesystem do
  let(:filesystem) { Masamune::MockFilesystem.new }
  let(:cached_filesystem) { Masamune::CachedFilesystem.new(filesystem) }

  context 'when path is present, top down traversal' do
    before do
      filesystem.touch!('/a/b/c/1.txt', '/a/b/c/2.txt', '/a/b/c/3.txt')
      expect(filesystem).to receive(:glob_stat).with('/a/b/*').once.and_call_original
      expect(filesystem).to receive(:glob_stat).with('/a').once.and_call_original
      expect(filesystem).to receive(:glob_stat).with('/*').never
    end

    it 'calls Filesystem#glob_stat once for multiple calls' do
      expect(cached_filesystem.exists?('/a/b/c/1.txt')).to eq(true)
      expect(cached_filesystem.exists?('/a/b/c/2.txt')).to eq(true)
      expect(cached_filesystem.exists?('/a/b/c/3.txt')).to eq(true)
      expect(cached_filesystem.exists?('/a/b/c/4.txt')).to eq(false)
      expect(cached_filesystem.exists?('/a/b/c')).to eq(true)
      expect(cached_filesystem.glob('/a/b/c/*')).not_to be_empty
      expect(cached_filesystem.glob('/a/b/c/*.txt')).not_to be_empty
      expect(cached_filesystem.stat('/a/b/c/1.txt')).to_not be_nil
      expect(cached_filesystem.stat('/a/b/c/2.txt')).to_not be_nil
      expect(cached_filesystem.stat('/a/b/c/3.txt')).to_not be_nil
      expect(cached_filesystem.stat('/a/b/c/4.txt')).to be_nil
      expect(cached_filesystem.stat('/a/b/c')).to_not be_nil
      expect(cached_filesystem.stat('/a/b')).to_not be_nil
      expect(cached_filesystem.stat('/a')).to_not be_nil
    end
  end

  context 'when path is present, bottom up traversal' do
    before do
      filesystem.touch!('/a/b/c/1.txt', '/a/b/c/2.txt', '/a/b/c/3.txt')
      expect(filesystem).to receive(:glob_stat).with('/a/*').once.and_call_original
      expect(filesystem).to receive(:glob_stat).with('/*').never
    end

    it 'calls Filesystem#glob_stat once for multiple calls' do
      expect(cached_filesystem.glob('/a/b/*')).to eq(['/a/b/c/1.txt', '/a/b/c/2.txt', '/a/b/c/3.txt'])
      expect(cached_filesystem.exists?('/a/b/c/1.txt')).to eq(true)
      expect(cached_filesystem.exists?('/a/b/c/2.txt')).to eq(true)
      expect(cached_filesystem.exists?('/a/b/c/3.txt')).to eq(true)
      expect(cached_filesystem.exists?('/a/b/c/4.txt')).to eq(false)
      expect(cached_filesystem.stat('/a/b/c/1.txt')).to_not be_nil
      expect(cached_filesystem.stat('/a/b/c/2.txt')).to_not be_nil
      expect(cached_filesystem.stat('/a/b/c/3.txt')).to_not be_nil
      expect(cached_filesystem.stat('/a/b/c/4.txt')).to be_nil
      expect(cached_filesystem.stat('/a/b/c')).to_not be_nil
      expect(cached_filesystem.stat('/a/b')).to_not be_nil
      expect(cached_filesystem.stat('/a')).to_not be_nil
    end
  end

  context 'when path is present, checking for similar non existant paths' do
    before do
      filesystem.touch!('/y=2013/m=1/d=22/00000')
      expect(filesystem).to receive(:glob_stat).with('/y=2013/m=1/*').once.and_call_original
      expect(filesystem).to receive(:glob_stat).with('/y=2013/*').once.and_call_original
      expect(filesystem).to receive(:glob_stat).with('/*').never
    end

    it 'calls Filesystem#glob_stat once for multiple calls' do
      expect(cached_filesystem.exists?('/y=2013/m=1/d=22/00000')).to eq(true)
      expect(cached_filesystem.exists?('/y=2013/m=1/d=22')).to eq(true)
      expect(cached_filesystem.exists?('/y=2013/m=1/d=2')).to eq(false)
      expect(cached_filesystem.glob('/y=2013/m=1/*')).not_to be_empty
      expect(cached_filesystem.glob('/y=2013/m=1/d=22/*')).not_to be_empty
      expect(cached_filesystem.stat('/y=2013/m=1/d=22/00000')).not_to be_nil
      expect(cached_filesystem.stat('/y=2013/m=1/d=22')).not_to be_nil
      expect(cached_filesystem.stat('/y=2013/m=1')).not_to be_nil
      expect(cached_filesystem.stat('/y=2013')).not_to be_nil
    end
  end

  context 'when path is present, checking for similar existing paths' do
    before do
      filesystem.touch!('/logs/box1_123.txt', '/logs/box2_123.txt', '/logs/box3_123.txt')
      expect(filesystem).to receive(:glob_stat).with('/logs/*').once.and_call_original
      expect(filesystem).to receive(:glob_stat).with('/*').never
    end

    it 'calls Filesystem#glob_stat once for multiple calls' do
      expect(cached_filesystem.exists?('/logs/box1_123.txt')).to eq(true)
      expect(cached_filesystem.exists?('/logs/box1_456.txt')).to eq(false)
      expect(cached_filesystem.exists?('/logs/box2_123.txt')).to eq(true)
      expect(cached_filesystem.exists?('/logs/box2_456.txt')).to eq(false)
      expect(cached_filesystem.exists?('/logs/box3_123.txt')).to eq(true)
      expect(cached_filesystem.exists?('/logs/box3_456.txt')).to eq(false)
      expect(cached_filesystem.exists?('/logs/box4_123.txt')).to eq(false)
      expect(cached_filesystem.exists?('/logs/box4_456.txt')).to eq(false)
      expect(cached_filesystem.exists?('/logs/box')).to eq(false)
      expect(cached_filesystem.glob('/logs/*')).not_to be_empty
      expect(cached_filesystem.glob('/logs/*.txt')).not_to be_empty
      expect(cached_filesystem.glob('/logs/box1_*.txt')).not_to be_empty
      expect(cached_filesystem.glob('/logs/box2_*.txt')).not_to be_empty
      expect(cached_filesystem.glob('/logs/box3_*.txt')).not_to be_empty
      expect(cached_filesystem.glob('/logs/box*.txt').size).to eq(3)
      expect(cached_filesystem.glob('/logs/box*.csv')).to be_empty
      expect(cached_filesystem.glob('/logs/box')).to be_empty
      expect(cached_filesystem.glob('/logs/box/*')).to be_empty
      expect(cached_filesystem.stat('/logs/box1_123.txt')).to_not be_nil
      expect(cached_filesystem.stat('/logs/box1_456.txt')).to be_nil
      expect(cached_filesystem.stat('/logs/box2_123.txt')).to_not be_nil
      expect(cached_filesystem.stat('/logs/box2_456.txt')).to be_nil
      expect(cached_filesystem.stat('/logs/box3_123.txt')).to_not be_nil
      expect(cached_filesystem.stat('/logs/box3_456.txt')).to be_nil
      expect(cached_filesystem.stat('/logs/box4_123.txt')).to be_nil
      expect(cached_filesystem.stat('/logs/box4_456.txt')).to be_nil
      expect(cached_filesystem.glob('/logs')).not_to be_nil
    end
  end

  context 'when path is missing' do
    before do
      filesystem.touch!('/a/b/c')
      expect(filesystem).to receive(:glob_stat).with('/a/b/*').once.and_call_original
      expect(filesystem).to receive(:glob_stat).with('/a').once.and_call_original
      expect(filesystem).to receive(:glob_stat).with('/*').never
    end

    it 'calls Filesystem#glob_stat once for multiple calls' do
      expect(cached_filesystem.exists?('/a/b/c/1.txt')).to eq(false)
      expect(cached_filesystem.exists?('/a/b/c/2.txt')).to eq(false)
      expect(cached_filesystem.exists?('/a/b/c/3.txt')).to eq(false)
      expect(cached_filesystem.exists?('/a/b/c')).to eq(true)
      expect(cached_filesystem.exists?('/a/b')).to eq(true)
      expect(cached_filesystem.exists?('/a')).to eq(true)
      expect(cached_filesystem.glob('/a')).to be_empty
      expect(cached_filesystem.glob('/a/*')).not_to be_empty
      expect(cached_filesystem.glob('/a/b')).to be_empty
      expect(cached_filesystem.glob('/a/b/*')).not_to be_empty
      expect(cached_filesystem.glob('/a/b/c')).not_to be_empty
      expect(cached_filesystem.glob('/a/b/c/*')).to be_empty
      expect(cached_filesystem.glob('/a/b/c/*.txt')).to be_empty
      expect(cached_filesystem.stat('/a/b/c/1.txt')).to be_nil
      expect(cached_filesystem.stat('/a/b/c/2.txt')).to be_nil
      expect(cached_filesystem.stat('/a/b/c/3.txt')).to be_nil
      expect(cached_filesystem.stat('/a/b/c')).to_not be_nil
      expect(cached_filesystem.stat('/a/b')).to_not be_nil
      expect(cached_filesystem.stat('/a')).to_not be_nil
    end
  end
end
