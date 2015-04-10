#  The MIT License (MIT)
#
#  Copyright (c) 2014-2015, VMware, Inc. All Rights Reserved.
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in
#  all copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
#  THE SOFTWARE.

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
      expect(cached_filesystem.glob('/a/b/c/*').count).to eq(3)
      expect(cached_filesystem.glob('/a/b/c/*.txt').count).to eq(3)
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
      expect(cached_filesystem.glob('/a/b/*')).to include '/a/b/c/1.txt'
      expect(cached_filesystem.glob('/a/b/*')).to include '/a/b/c/2.txt'
      expect(cached_filesystem.glob('/a/b/*')).to include '/a/b/c/3.txt'
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
      expect(cached_filesystem.glob('/y=2013/m=1/*').count).to eq(1)
      expect(cached_filesystem.glob('/y=2013/m=1/d=22/*').count).to eq(1)
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
      expect(cached_filesystem.glob('/logs/*').count).to eq(3)
      expect(cached_filesystem.glob('/logs/*.txt').count).to eq(3)
      expect(cached_filesystem.glob('/logs/box1_*.txt').count).to eq(1)
      expect(cached_filesystem.glob('/logs/box2_*.txt').count).to eq(1)
      expect(cached_filesystem.glob('/logs/box3_*.txt').count).to eq(1)
      expect(cached_filesystem.glob('/logs/box*.txt').count).to eq(3)
      expect(cached_filesystem.glob('/logs/box*.csv').count).to eq(0)
      expect(cached_filesystem.glob('/logs/box').count).to eq(0)
      expect(cached_filesystem.glob('/logs/box/*').count).to eq(0)
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
      expect(cached_filesystem.glob('/a').count).to eq(0)
      expect(cached_filesystem.glob('/a/*').count).to eq(1)
      expect(cached_filesystem.glob('/a/b').count).to eq(0)
      expect(cached_filesystem.glob('/a/b/*').count).to eq(1)
      expect(cached_filesystem.glob('/a/b/c').count).to eq(1)
      expect(cached_filesystem.glob('/a/b/c/*').count).to eq(0)
      expect(cached_filesystem.glob('/a/b/c/*.txt').count).to eq(0)
      expect(cached_filesystem.stat('/a/b/c/1.txt')).to be_nil
      expect(cached_filesystem.stat('/a/b/c/2.txt')).to be_nil
      expect(cached_filesystem.stat('/a/b/c/3.txt')).to be_nil
      expect(cached_filesystem.stat('/a/b/c')).to_not be_nil
      expect(cached_filesystem.stat('/a/b')).to_not be_nil
      expect(cached_filesystem.stat('/a')).to_not be_nil
    end
  end
end
