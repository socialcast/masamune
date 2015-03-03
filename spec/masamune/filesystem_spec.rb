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

require 'securerandom'

# NOTE when operating between hdfs and s3, hadoop fs requires s3n URI
# See: http://wiki.apache.org/hadoop/AmazonS3
shared_examples_for 'Filesystem' do
  let(:filesystem) { Masamune::Filesystem.new }

  let(:tmp_dir) { File.join(Dir.tmpdir, SecureRandom.hex, SecureRandom.hex) }
  let(:old_dir) { File.join(tmp_dir, SecureRandom.hex) }
  let(:new_dir) { File.join(tmp_dir, SecureRandom.hex) }
  let(:other_new_dir) { File.join(tmp_dir, SecureRandom.hex) }
  let(:new_file) { File.join(old_dir, SecureRandom.hex) }
  let(:other_new_file) { File.join(old_dir, SecureRandom.hex) }
  let(:old_file) { File.join(old_dir, SecureRandom.hex + '.txt') }

  before do
    filesystem.configuration.retries = 0
    FileUtils.mkdir_p(old_dir)
    FileUtils.touch(old_file)
  end

  after do
    FileUtils.rmdir(tmp_dir)
  end

  describe '#get_path' do
    context 'after add_path is called' do
      before do
        instance.add_path(:home_dir, '/home')
      end
      it { expect(instance.get_path(:home_dir)).to eq('/home') }

      context 'with extra directories' do
        it { expect(instance.get_path(:home_dir, 'a', 'b', 'c')).to eq('/home/a/b/c') }
      end

      context 'with extra directories delimited by "/"' do
        it { expect(instance.get_path(:home_dir, '/a/b', 'c')).to eq('/home/a/b/c') }
      end

      context 'with parameter substitution' do
        before do
          instance.configuration.params[:user] = 'zombo'
          instance.add_path(:user_path, '/home/%user/files')
        end
        it { expect(instance.get_path(:user_path)).to eq('/home/zombo/files') }

        context 'in extra section' do
          before do
            instance.configuration.params[:file] = 'anything_is_possible.txt'
          end
          it { expect(instance.get_path(:user_path, '%file')).to eq('/home/zombo/files/anything_is_possible.txt') }
        end
      end
    end

    context 'before add_path is called' do
      it { expect(instance.get_path(:home_dir)).to be_a(Proc) }
    end
  end

  describe '#parent_paths' do
    subject { instance.parent_paths(path) }

    context 'with local blank' do
      let(:path) { '' }
      it { is_expected.to eq([]) }
    end

    context 'with local path with slash' do
      let(:path) { '/a/b/c' }
      it { is_expected.to eq(['/', '/a', '/a/b']) }
    end

    context 'with local path without slash' do
      let(:path) { 'a/b/c' }
      it { is_expected.to eq(['a', 'a/b']) }
    end

    context 'with s3 bucket with blank' do
      let(:path) { 's3://bucket' }
      it { is_expected.to eq([]) }
    end

    context 'with s3 bucket with slash' do
      let(:path) { 's3://bucket/' }
      it { is_expected.to eq([]) }
    end

    context 'with s3 bucket with path' do
      let(:path) { 's3://bucket/a/b/c' }
      it { is_expected.to eq(['s3://bucket/', 's3://bucket/a', 's3://bucket/a/b']) }
    end

    context 'with hdfs directory with path' do
      let(:path) { 'hdfs:///a/b/c' }
      it { is_expected.to eq(['hdfs:///', 'hdfs:///a', 'hdfs:///a/b']) }
    end
  end

  describe '#root_path?' do
    subject { instance.root_path?(path) }

    context 'with nil' do
      let(:path) { nil }
      it { expect { |b| subject }.to raise_error ArgumentError }
    end

    context 'with blank' do
      let(:path) { ' ' }
      it { expect { |b| subject }.to raise_error ArgumentError }
    end

    context 'with empty' do
      let(:path) { '' }
      it { expect { |b| subject }.to raise_error ArgumentError }
    end

    context 'with relative path' do
      let(:path) { 'tmp' }
      it { expect { |b| subject }.to raise_error ArgumentError }
    end

    context 'with local root' do
      let(:path) { '/' }
      it { is_expected.to eq(true) }
    end

    context 'with local non-root' do
      let(:path) { '/tmp' }
      it { is_expected.to eq(false) }
    end

    context 'with hdfs root' do
      let(:path) { 'file:///' }
      it { is_expected.to eq(true) }
    end

    context 'with hdfs non-root' do
      let(:path) { 'file:///tmp' }
      it { is_expected.to eq(false) }
    end

    context 'with s3 root' do
      let(:path) { 's3://bucket/' }
      it { is_expected.to eq(true) }
    end

    context 'with s3 non-root' do
      let(:path) { 's3://bucket/tmp' }
      it { is_expected.to eq(false) }
    end

    context 'with s3 bucket' do
      let(:path) { 's3://bucket' }
      it { is_expected.to eq(true) }
    end
  end

  describe '#resolve_file' do
    subject { instance.resolve_file(paths) }

    context 'with nil' do
      let(:paths) { nil }
      it { is_expected.to be_nil }
    end

    context 'with empty' do
      let(:paths) { [] }
      it { is_expected.to be_nil }
    end

    context 'with one file' do
      let(:paths) { old_file }
      it { is_expected.to eq(old_file) }
    end

    context 'with directories and file' do
      let(:paths) { [old_dir, new_dir, new_file, old_file] }
      it { is_expected.to eq(old_file) }
    end
  end

  describe '#dirname' do
    subject { instance.dirname(path) }

    context 'with local blank' do
      let(:path) { '' }
      it { is_expected.to be_blank }
    end

    context 'with local path with slash' do
      let(:path) { '/a/b/c' }
      it { is_expected.to eq('/a/b') }
    end

    context 'with local path without slash' do
      let(:path) { 'a/b/c' }
      it { is_expected.to eq('a/b') }
    end

    context 'with local relative path' do
      let(:path) { '/a/b/../c' }
      it { is_expected.to eq('/a/c') }
    end

    context 'with s3 bucket with blank' do
      let(:path) { 's3://bucket' }
      it { is_expected.to eq('s3://bucket') }
    end

    context 'with s3 bucket with slash' do
      let(:path) { 's3://bucket/' }
      it { is_expected.to eq('s3://bucket/') }
    end

    context 'with s3 bucket with path' do
      let(:path) { 's3://bucket/a/b/c' }
      it { is_expected.to eq('s3://bucket/a/b') }
    end

    context 'with s3 bucket with relative path' do
      let(:path) { 's3://bucket/a/b/../c' }
      it { is_expected.to eq('s3://bucket/a/c') }
    end

    context 'with hdfs directory with path' do
      let(:path) { 'hdfs:///a/b/c' }
      it { is_expected.to eq('hdfs:///a/b') }
    end

    context 'with hdfs directory with path' do
      let(:path) { 'hdfs:///a/b/c' }
      it { is_expected.to eq('hdfs:///a/b') }
    end

    context 'with hdfs directory with relative path' do
      let(:path) { 'hdfs:///a/b/../c' }
      it { is_expected.to eq('hdfs:///a/c') }
    end
  end

  describe '#basename' do
    subject { instance.basename(path) }

    context 'with local blank' do
      let(:path) { '' }
      it { is_expected.to be_blank }
    end

    context 'with local path with slash' do
      let(:path) { '/a/b/c' }
      it { is_expected.to eq('c') }
    end

    context 'with local path without slash' do
      let(:path) { 'a/b/c' }
      it { is_expected.to eq('c') }
    end

    context 'with local relative path' do
      let(:path) { '/a/b/../c' }
      it { is_expected.to eq('c') }
    end

    context 'with s3 bucket with blank' do
      let(:path) { 's3://bucket' }
      it { is_expected.to be_nil }
    end

    context 'with s3 bucket with slash' do
      let(:path) { 's3://bucket/' }
      it { is_expected.to be_nil }
    end

    context 'with s3 bucket with path' do
      let(:path) { 's3://bucket/a/b/c' }
      it { is_expected.to eq('c') }
    end

    context 'with s3 bucket with relative path' do
      let(:path) { 's3://bucket/a/b/../c' }
      it { is_expected.to eq('c') }
    end

    context 'with hdfs directory with path' do
      let(:path) { 'hdfs:///a/b/c' }
      it { is_expected.to eq('c') }
    end

    context 'with hdfs directory with path' do
      let(:path) { 'hdfs:///a/b/c' }
      it { is_expected.to eq('c') }
    end

    context 'with hdfs directory with relative path' do
      let(:path) { 'hdfs:///a/b/../c' }
      it { is_expected.to eq('c') }
    end
  end

  describe '#touch!' do
    subject do
      File.exists?(new_file) && File.exists?(other_new_file)
    end

    context 'local' do
      before do
        instance.touch!(new_file, other_new_file)
      end
      it { is_expected.to eq(true) }
    end

    context 'hdfs' do
      before do
        instance.touch!('file://' + new_file, 'file://' + other_new_file)
      end
      it { is_expected.to eq(true) }
    end

    context 's3' do
      before do
        expect(filesystem).to receive(:s3cmd).with('put', an_instance_of(String), 's3://bucket/file').at_most(:once)
        expect(filesystem).to receive(:s3cmd).with('put', an_instance_of(String), 's3://bucket/other_file').at_most(:once)
        instance.touch!('s3://bucket/file', 's3://bucket/other_file')
      end

      it 'meets expectations' do; end
    end
  end

  describe '#exists?' do
    context 'local missing file' do
      subject { instance.exists?(new_file) }
      it { is_expected.to eq(false) }
    end

    context 'hdfs missing file' do
      subject { instance.exists?('file://' + new_file) }
      it { is_expected.to eq(false) }
    end

    context 'local existing file' do
      subject { instance.exists?(old_file) }
      it { is_expected.to eq(true) }
    end

    context 'hdfs existing file' do
      subject { instance.exists?('file://' + old_file) }
      it { is_expected.to eq(true) }
    end

    context 's3 existing file' do
      before do
        expect(filesystem).to receive(:s3cmd).with('ls', 's3://bucket/00', safe: true).at_most(:once).
          and_yield(%q(2013-05-24 18:52      2912   s3://bucket/00)).
          and_yield(%q(2013-05-24 18:52      2912   s3://bucket/01))
        expect(filesystem).to receive(:s3cmd).with('ls', '--recursive', 's3://bucket/00', safe: true).at_most(:once).
          and_yield(%q(2013-05-24 18:52      2912   s3://bucket/00)).
          and_yield(%q(2013-05-24 18:52      2912   s3://bucket/01))
      end

      subject { instance.exists?('s3://bucket/00') }

      it { is_expected.to eq(true) }
    end

    context 's3 missing file' do
      before do
        expect(filesystem).to receive(:s3cmd).with('ls', 's3://bucket/0', safe: true).at_most(:once).
          and_yield(%q(2013-05-24 18:52      2912   s3://bucket/00)).
          and_yield(%q(2013-05-24 18:52      2912   s3://bucket/01))
        expect(filesystem).to receive(:s3cmd).with('ls', '--recursive', 's3://bucket/0', safe: true).at_most(:once).
          and_yield(%q(2013-05-24 18:52      2912   s3://bucket/00)).
          and_yield(%q(2013-05-24 18:52      2912   s3://bucket/01))
      end

      subject { instance.exists?('s3://bucket/0') }

      it { is_expected.to eq(false) }
    end
  end

  describe '#stat' do
    subject(:stat) { result }
    context 'local missing file' do
      let(:result) { instance.stat(new_file) }
      it { is_expected.to be_nil }
    end

    context 'hdfs missing file' do
      let(:result) { instance.stat('file://' + new_file) }
      it { is_expected.to be_nil }
    end

    context 's3 missing file' do
      before do
        expect(filesystem).to receive(:s3cmd).with('ls', 's3://bucket/', safe: true).at_most(:once)
        expect(filesystem).to receive(:s3cmd).with('ls', '--recursive', %r{s3://bucket/[\*|file.txt]}, safe: true).
          and_yield('')
      end
      let(:result) { instance.stat('s3://bucket/file.txt') }
      it { is_expected.to be_nil }
    end

    context 'local existing file' do
      let(:result) { instance.stat(old_file) }

      describe '#name' do
        subject { stat.name }
        it { is_expected.to eq(old_file) }
      end

      describe '#mtime' do
        subject { stat.mtime }
        it { is_expected.to eq(File.stat(old_file).mtime.at_beginning_of_minute.utc) }
      end

      describe '#mtime' do
        subject { stat.mtime }
        it { is_expected.to be_a(Time) }
      end

      describe '#size' do
        subject { stat.size }
        it { is_expected.to be_an(Integer) }
      end
    end

    context 'local existing file with glob' do
      let(:result) { instance.stat(File.join(old_dir, '*')) }
      it { expect { result }.to raise_error ArgumentError }
    end

    context 'local existing file (recursive)' do
      let(:result) { instance.stat(File.join(tmp_dir, '*')) }
      it { expect { result }.to raise_error /cannot contain wildcard/ }
    end

    context 'hdfs existing file' do
      let(:result) { instance.stat('file://' + old_file) }

      describe '#name' do
        subject { stat.name }
        it { is_expected.to eq('file://' + old_file) }
      end

      describe '#mtime' do
        subject { stat.mtime }
        it { is_expected.to eq(File.stat(old_file).mtime.at_beginning_of_minute.utc) }
      end

      describe '#mtime' do
        subject { stat.mtime }
        it { is_expected.to be_a(Time) }
      end

      describe '#size' do
        subject { stat.size }
        it { is_expected.to be_an(Integer) }
      end
    end

    context 's3 existing file' do
      before do
        expect(filesystem).to receive(:s3cmd).with('ls', '--recursive', %r{s3://bucket/[\*|file.txt]}, safe: true).
          and_yield(%q(2013-05-24 18:52      2912   s3://bucket/file.txt))
      end
      let(:result) { instance.stat('s3://bucket/file.txt') }

      describe '#name' do
        subject { stat.name }
        it { is_expected.to eq('s3://bucket/file.txt') }
      end

      describe '#mtime' do
        subject { stat.mtime }
        it { is_expected.to eq(Time.parse('2013-05-24 18:52:00 +0000')) }
      end

      describe '#mtime' do
        subject { stat.mtime }
        it { is_expected.to be_a(Time) }
      end

      describe '#size' do
        subject { stat.size }
        it { is_expected.to eq(2912) }
      end
    end

    context 's3 existing directory' do
      before do
        expect(filesystem).to receive(:s3cmd).with('ls', '--recursive', %r{s3://bucket/[\*|dir]}, safe: true).
          and_yield(%q(2013-05-24 18:52      2912   s3://bucket/dir/file.txt))
      end

      let(:result) { instance.stat('s3://bucket/dir') }

      describe '#name' do
        subject { stat.name }
        it { is_expected.to eq('s3://bucket/dir/file.txt') }
      end

      describe '#mtime' do
        subject { stat.mtime }
        it { is_expected.to eq(Time.parse('2013-05-24 18:52:00 +0000')) }
      end

      describe '#mtime' do
        subject { stat.mtime }
        it { is_expected.to be_a(Time) }
      end

      describe '#size' do
        subject { stat.size }
        it { is_expected.to eq(2912) }
      end
    end
  end

  describe '#mkdir!' do
    subject do
      Dir.exists?(new_dir) && Dir.exists?(other_new_dir)
    end

    context 'local directory' do
      before do
        instance.mkdir!(new_dir, other_new_dir)
      end
      it { is_expected.to eq(true) }
    end

    context 'hdfs directory' do
      before do
        instance.mkdir!('file://' + new_dir, 'file://' + other_new_dir)
      end
      it { is_expected.to eq(true) }
    end

    context 's3 directory' do
      before do
        expect(filesystem).to receive(:s3cmd).with('put', an_instance_of(String), 's3://bucket/dir/.not_empty').at_most(:once)
        expect(filesystem).to receive(:s3cmd).with('put', an_instance_of(String), 's3://bucket/other_dir/.not_empty').at_most(:once)
        instance.mkdir!('s3://bucket/dir', 's3://bucket/other_dir')
      end

      it 'meets expectations' do; end
    end
  end

  describe '#glob' do
    subject do
      instance.glob(pattern)
    end

    context 'local no matches' do
      let(:pattern) { File.join(new_dir, '*') }
      it { is_expected.to be_empty }
      it { expect { |b| instance.glob(pattern, &b) }.to_not yield_control }
    end

    context 'local one matches' do
      let(:pattern) { File.join(File.dirname(old_file), '*') }
      it { is_expected.not_to be_empty }
      it { expect { |b| instance.glob(pattern, &b) }.to yield_with_args(old_file) }
    end

    context 'local one matches (recursive)' do
      let(:pattern) { File.join(tmp_dir, '*') }
      it 'has 2 items' do
        expect(subject.size).to eq(2)
      end
      it { is_expected.to include old_dir }
      it { is_expected.to include old_file }
      it { expect { |b| instance.glob(pattern, &b) }.to yield_successive_args(old_dir, old_file) }
    end

    context 'local one matches (with suffix)' do
      let(:pattern) { File.join(File.dirname(old_file), '*.txt') }
      it 'has 1 item' do
        expect(subject.size).to eq(1)
      end
      it { is_expected.to include old_file }
      it { expect { |b| instance.glob(pattern, &b) }.to yield_with_args(old_file) }
    end

    context 'hdfs no matches' do
      let(:pattern) { File.join(new_dir, '*') }
      it { is_expected.to be_empty }
      it { expect { |b| instance.glob('file://' + pattern, &b) }.to_not yield_control }
    end

    context 'hdfs one matches' do
      let(:pattern) { File.join(File.dirname(old_file), '*') }
      it { is_expected.not_to be_empty }
      it { expect { |b| instance.glob('file://' + pattern, &b) }.to yield_with_args('file://' + old_file) }
    end

    context 's3 no matches' do
      let(:pattern) { 's3://bucket/dir/*.txt' }

      before do
        expect(filesystem).to receive(:s3cmd).with('ls', '--recursive', "s3://bucket/dir", safe: true).at_most(:once)
        expect(filesystem).to receive(:s3cmd).with('ls', '--recursive', "s3://bucket/dir/*", safe: true).at_most(:once)
      end

      it { is_expected.to be_empty }
    end

    context 's3 no matches with implicit glob results' do
      let(:pattern) { 's3://bucket/dir/0' }

      before do
        expect(filesystem).to receive(:s3cmd).with('ls', '--recursive', %r{s3://bucket/[\*|dir/*]}, safe: true).
          and_yield(%q(2013-05-24 18:52      2912   s3://bucket/dir/01.txt)).
          and_yield(%q(2013-05-24 18:53      2912   s3://bucket/dir/02.txt))
      end

      it { is_expected.to be_empty }
    end

    context 's3 one matches' do
      let(:pattern) { 's3://bucket/dir/*.txt' }

      before do
        expect(filesystem).to receive(:s3cmd).with('ls', '--recursive', %r{s3://bucket/[\*|dir/*]}, safe: true).
          and_yield(%q(2013-05-24 18:52      2912   s3://bucket/dir/file.txt)).
          and_yield(%q(2013-05-24 18:53      2912   s3://bucket/dir/file.csv))
      end

      it { is_expected.to include 's3://bucket/dir/file.txt' }
      it { is_expected.not_to include 's3://bucket/dir/file.csv' }
    end

    context 's3 many matches' do
      let(:pattern) { 's3://bucket/dir/*' }

      before do
        expect(filesystem).to receive(:s3cmd).with('ls', '--recursive', %r{s3://bucket/[\*|dir/*]}, safe: true).
          and_yield(%q(                       DIR   s3://bucket/dir/file_$folder$)).
          and_yield(%q(2013-05-24 18:52      2912   s3://bucket/dir/file.txt)).
          and_yield(%q(2013-05-24 18:53      2912   s3://bucket/dir/file.csv))
      end

      it { is_expected.to include 's3://bucket/dir/file.txt' }
      it { is_expected.to include 's3://bucket/dir/file.csv' }
    end
  end

  describe '#glob_sort' do
    before do
      allow_any_instance_of(Masamune::Filesystem).to receive(:glob).and_return(%w(/tmp/a/02.txt /tmp/b/01.txt /tmp/c/00.txt))
    end

    subject do
      instance.glob_sort('/tmp/*', order: :basename)
    end

    it { is_expected.to eq(%w(/tmp/c/00.txt /tmp/b/01.txt /tmp/a/02.txt)) }
  end

  describe '#copy_file_to_file' do
    let(:result_file) { File.join(new_dir, File.basename(old_file)) }

    subject do
      File.exists?(result_file)
    end

    context 'local file to local file' do
      before do
        instance.copy_file_to_file(old_file, result_file)
      end

      it { is_expected.to eq(true) }
    end

    context 'local file to s3 file' do
      before do
        expect(filesystem).to receive(:s3cmd).with('put', old_file, 's3://bucket/new_dir/new_file')
        instance.copy_file_to_file(old_file, 's3://bucket/new_dir/new_file')
      end

      it 'meets expectations' do; end
    end

    context 'local file to hdfs file' do
      before do
        instance.copy_file_to_file(old_file, 'file://' + result_file)
      end

      it { is_expected.to eq(true) }
    end

    context 'hdfs file to hdfs file' do
      before do
        instance.copy_file_to_file('file://' + old_file, 'file://' + result_file)
      end

      it { is_expected.to eq(true) }
    end

    context 'hdfs file to local file' do
      before do
        instance.copy_file_to_file('file://' + old_file, result_file)
      end

      it { is_expected.to eq(true) }
    end

    context 'hdfs file to s3 file' do
      before do
        expect(filesystem).to receive(:hadoop_fs).with('-cp', 'file://' + old_file, 's3n://bucket/new_dir/new_file')
        instance.copy_file_to_file('file://' + old_file, 's3://bucket/new_dir/new_file')
      end

      it 'meets expectations' do; end
    end

    context 's3 file to s3 file' do
      before do
        expect(filesystem).to receive(:s3cmd).with('cp', 's3://bucket/old_file', 's3://bucket/new_dir/new_file')
        instance.copy_file_to_file('s3://bucket/old_file', 's3://bucket/new_dir/new_file')
      end

      it 'meets expectations' do; end
    end

    context 's3 file to local file' do
      before do
        expect(filesystem).to receive(:s3cmd).with('get', 's3://bucket/old_file', new_file)
        instance.copy_file_to_file('s3://bucket/old_file', new_file)
      end

      it 'meets expectations' do; end
    end

    context 's3 file to hdfs file' do
      before do
        expect(filesystem).to receive(:hadoop_fs).with('-mkdir', '-p', 'file://' + new_dir)
        expect(filesystem).to receive(:hadoop_fs).with('-cp', 's3n://bucket/old_file', 'file://' + result_file)
        instance.copy_file_to_file('s3://bucket/old_file', 'file://' + result_file)
      end

      it 'meets expectations' do; end
    end
  end

  describe '#copy_file_to_dir' do
    let(:result_file) { File.join(new_dir, File.basename(old_file)) }

    subject do
      File.exists?(result_file)
    end

    context 'local file to local dir' do
      before do
        instance.copy_file_to_dir(old_file, new_dir)
      end

      it { is_expected.to eq(true) }
    end

    context 'local file to same local dir' do
      before do
        instance.copy_file_to_dir(old_file, old_dir)
      end

      it { is_expected.to eq(false) }
    end

    context 'local file to s3 dir' do
      before do
        expect(filesystem).to receive(:s3cmd).with('put', old_file, 's3://bucket/new_dir/')
        instance.copy_file_to_dir(old_file, 's3://bucket/new_dir')
      end

      it 'meets expectations' do; end
    end

    context 'local file to hdfs dir' do
      before do
        instance.copy_file_to_dir(old_file, 'file://' + new_dir)
      end

      it { is_expected.to eq(true) }
    end

    context 'hdfs file to hdfs dir' do
      before do
        instance.copy_file_to_dir('file://' + old_file, 'file://' + new_dir)
      end

      it { is_expected.to eq(true) }
    end

    context 'hdfs file to local dir' do
      before do
        instance.copy_file_to_dir('file://' + old_file, new_dir)
      end

      it { is_expected.to eq(true) }
    end

    context 'hdfs file to s3 dir' do
      before do
        expect(filesystem).to receive(:hadoop_fs).with('-cp', 'file://' + old_file, 's3n://bucket/new_dir')
        instance.copy_file_to_dir('file://' + old_file, 's3://bucket/new_dir')
      end

      it 'meets expectations' do; end
    end

    context 's3 file to s3 dir' do
      before do
        expect(filesystem).to receive(:s3cmd).with('cp', 's3://bucket/old_file', 's3://bucket/new_dir/')
        instance.copy_file_to_dir('s3://bucket/old_file', 's3://bucket/new_dir')
      end

      it 'meets expectations' do; end
    end

    context 's3 file to local dir' do
      before do
        expect(filesystem).to receive(:s3cmd).with('get', 's3://bucket/old_file', new_dir)
        instance.copy_file_to_dir('s3://bucket/old_file', new_dir)
      end

      it 'meets expectations' do; end
    end

    context 's3 file to hdfs dir' do
      before do
        expect(filesystem).to receive(:hadoop_fs).with('-mkdir', '-p', 'file://' + new_dir)
        expect(filesystem).to receive(:hadoop_fs).with('-cp', 's3n://bucket/old_file', 'file://' + new_dir)
        instance.copy_file_to_dir('s3://bucket/old_file', 'file://' + new_dir)
      end

      it 'meets expectations' do; end
    end
  end

  describe '#copy_dir' do
    subject do
      File.exists?(File.join(new_dir, File.basename(old_dir), File.basename(old_file)))
    end

    context 'local dir to local dir' do
      before do
        instance.copy_dir(old_dir, new_dir)
      end

      it { is_expected.to eq(true) }
    end

    context 'local dir to s3 dir' do
      before do
        expect(filesystem).to receive(:s3cmd).with('put', '--recursive', old_dir, 's3://bucket/new_dir/')
        instance.copy_dir(old_dir, 's3://bucket/new_dir')
      end

      it 'meets expectations' do; end
    end

    context 'local dir to hdfs dir' do
      before do
        instance.copy_dir(old_dir, 'file://' + new_dir)
      end

      it { is_expected.to eq(true) }
    end

    context 'hdfs dir to hdfs dir' do
      before do
        instance.copy_dir('file://' + old_dir, 'file://' + new_dir)
      end

      it { is_expected.to eq(true) }
    end

    context 'hdfs dir to local dir' do
      before do
        instance.copy_dir('file://' + old_dir, new_dir)
      end

      it { is_expected.to eq(true) }
    end

    context 'hdfs dir to s3 dir' do
      before do
        expect(filesystem).to receive(:hadoop_fs).with('-cp', 'file://' + old_dir, 's3n://bucket/new_dir')
        instance.copy_dir('file://' + old_dir, 's3://bucket/new_dir')
      end

      it 'meets expectations' do; end
    end

    context 's3 dir to s3 dir' do
      before do
        expect(filesystem).to receive(:s3cmd).with('cp', '--recursive', 's3://bucket/old_dir/', 's3://bucket/new_dir/')
        instance.copy_dir('s3://bucket/old_dir', 's3://bucket/new_dir')
      end

      it 'meets expectations' do; end
    end

    context 's3 dir to local dir' do
      before do
        expect(filesystem).to receive(:s3cmd).with('get', '--recursive', '--skip-existing', 's3://bucket/old_dir/', File.join(new_dir, 'old_dir'))
        instance.copy_dir('s3://bucket/old_dir', new_dir)
      end

      it 'meets expectations' do; end
    end

    context 's3 dir to hdfs dir' do
      before do
        expect(filesystem).to receive(:hadoop_fs).with('-mkdir', '-p', 'file://' + new_dir)
        expect(filesystem).to receive(:hadoop_fs).with('-cp', 's3n://bucket/old_dir', 'file://' + new_dir)
        instance.copy_dir('s3://bucket/old_dir', 'file://' + new_dir)
      end

      it 'meets expectations' do; end
    end
  end

  describe '#remove_file' do
    subject do
      File.exists?(old_file)
    end

    context 'local false' do
      before do
        instance.remove_file(old_file)
      end

      it { is_expected.to eq(false)}
    end

    context 'hdfs file' do
      before do
        instance.remove_file('file://' + old_file)
      end

      it { is_expected.to eq(false)}
    end

    context 's3 file' do
      before do
        expect(filesystem).to receive(:s3cmd).with('del', 's3://bucket/file')
        instance.remove_file('s3://bucket/file')
      end

      it 'meets expectations' do; end
    end
  end

  describe '#remove_dir' do
    subject do
      File.exists?(old_dir)
    end

    context 'local dir' do
      before do
        expect(filesystem).to receive(:root_path?).once.and_return(false)
        instance.remove_dir(old_dir)
      end

      it { is_expected.to eq(false)}
    end

    context 'local root dir' do
      before do
        expect(filesystem).to receive(:root_path?).once.and_return(true)
      end

      it { expect { instance.remove_dir(old_dir) }.to raise_error /root path/ }
    end

    context 'hdfs dir' do
      before do
        expect(filesystem).to receive(:root_path?).once.and_return(false)
        instance.remove_dir('file://' + old_dir)
      end

      it { is_expected.to eq(false)}
    end

    context 'hdfs root dir' do
      before do
        expect(filesystem).to receive(:root_path?).once.and_return(true)
      end

      it { expect { instance.remove_dir('file://' + old_dir) }.to raise_error /root path/ }
    end

    context 's3 dir' do
      before do
        expect(filesystem).to receive(:s3cmd).with('del', '--recursive', 's3://bucket/dir/')
        expect(filesystem).to receive(:s3cmd).with('del', '--recursive', 's3://bucket/dir_$folder$')
        instance.remove_dir('s3://bucket/dir')
      end

      it 'meets expectations' do; end
    end

    context 's3 root dir' do
      before do
        expect(filesystem).to receive(:s3cmd).never
      end

      it { expect { instance.remove_dir('s3://bucket/') }.to raise_error /root path/ }

      it 'meets expectations' do; end
    end
  end

  describe '#move_file_to_file' do
    subject(:removes_old_file) do
      !File.exists?(old_file)
    end

    subject(:creates_new_file) do
      File.exists?(new_file)
    end

    context 'local file to local file' do
      before do
        expect(FileUtils).to receive(:chmod).once
        instance.move_file_to_file(old_file, new_file)
      end

      it { expect(removes_old_file).to eq(true) }
      it { expect(creates_new_file).to eq(true) }
    end

    context 'local file to s3 file' do
      before do
        expect(filesystem).to receive(:s3cmd).with('put', old_file, 's3://bucket/new_dir/new_file')
        instance.move_file_to_file(old_file, 's3://bucket/new_dir/new_file')
      end

      it 'meets expectations' do; end
      it { expect(removes_old_file).to eq(true) }
    end

    context 'local file to hdfs file' do
      before do
        instance.move_file_to_file(old_file, 'file://' + new_file)
      end

      it { expect(removes_old_file).to eq(true) }
      it { expect(creates_new_file).to eq(true) }
    end

    context 'hdfs file to hdfs file' do
      before do
        instance.move_file_to_file('file://' + old_file, 'file://' + new_file)
      end

      it { expect(removes_old_file).to eq(true) }
      it { expect(creates_new_file).to eq(true) }
    end

    context 'hdfs file to local file' do
      before do
        instance.move_file_to_file('file://' + old_file, new_file)
      end

      it { expect(removes_old_file).to eq(true) }
      it { expect(creates_new_file).to eq(true) }
    end

    context 'hdfs file to s3 file' do
      before do
        expect(filesystem).to receive(:hadoop_fs).with('-cp', 'file://' + old_file, 's3n://bucket/new_dir/new_file')
        expect(filesystem).to receive(:hadoop_fs).with('-rm', 'file://' + old_file)
        instance.move_file_to_file('file://' + old_file, 's3://bucket/new_dir/new_file')
      end

      it 'meets expectations' do; end
    end

    context 's3 file to s3 file' do
      before do
        expect(filesystem).to receive(:s3cmd).with('mv', 's3://bucket/old_file', 's3://bucket/new_dir/new_file')
        instance.move_file_to_file('s3://bucket/old_file', 's3://bucket/new_dir/new_file')
      end

      it 'meets expectations' do; end
    end

    context 's3 file to local file' do
      before do
        expect(filesystem).to receive(:s3cmd).with('get', 's3://bucket/old_file', new_file)
        expect(filesystem).to receive(:s3cmd).with('del', 's3://bucket/old_file')
        instance.move_file_to_file('s3://bucket/old_file', new_file)
      end

      it 'meets expectations' do; end
    end

    context 's3 file to hdfs file' do
      before do
        expect(filesystem).to receive(:hadoop_fs).with('-mkdir', '-p', 'file://' + File.dirname(new_file))
        expect(filesystem).to receive(:hadoop_fs).with('-mv', 's3n://bucket/old_file', 'file://' + new_file)
        instance.move_file_to_file('s3://bucket/old_file', 'file://' + new_file)
      end

      it 'meets expectations' do; end
    end
  end

  describe '#move_file_to_dir' do
    before do
      FileUtils.mkdir_p(new_dir)
    end

    subject(:removes_old_file) do
      !File.exists?(old_file)
    end

    subject(:creates_new_file) do
      File.exists?(File.join(new_dir, File.basename(old_file)))
    end

    context 'local file to local dir' do
      before do
        expect(FileUtils).to receive(:chmod).once
        instance.move_file_to_dir(old_file, new_dir)
      end

      it { expect(removes_old_file).to eq(true) }
      it { expect(creates_new_file).to eq(true) }
    end

    context 'local file to s3 dir' do
      before do
        expect(filesystem).to receive(:s3cmd).with('put', old_file, 's3://bucket/new_dir/')
        instance.move_file_to_dir(old_file, 's3://bucket/new_dir')
      end

      it 'meets expectations' do; end
      it { expect(removes_old_file).to eq(true) }
    end

    context 'local file to hdfs dir' do
      before do
        instance.move_file_to_dir(old_file, 'file://' + new_dir)
      end

      it { expect(removes_old_file).to eq(true) }
      it { expect(creates_new_file).to eq(true) }
    end

    context 'hdfs file to hdfs dir' do
      before do
        instance.move_file_to_dir('file://' + old_file, 'file://' + new_dir)
      end

      it { expect(removes_old_file).to eq(true) }
      it { expect(creates_new_file).to eq(true) }
    end

    context 'hdfs file to local dir' do
      before do
        instance.move_file_to_dir('file://' + old_file, new_dir)
      end

      it { expect(removes_old_file).to eq(true) }
      it { expect(creates_new_file).to eq(true) }
    end

    context 'hdfs file to s3 dir' do
      before do
        expect(filesystem).to receive(:hadoop_fs).with('-cp', 'file://' + old_file, 's3n://bucket/new_dir/')
        expect(filesystem).to receive(:hadoop_fs).with('-rm', 'file://' + old_file)
        instance.move_file_to_dir('file://' + old_file, 's3://bucket/new_dir')
      end

      it 'meets expectations' do; end
    end

    context 's3 file to s3 dir' do
      before do
        expect(filesystem).to receive(:s3cmd).with('mv', 's3://bucket/old_file', 's3://bucket/new_dir/')
        instance.move_file_to_dir('s3://bucket/old_file', 's3://bucket/new_dir')
      end

      it 'meets expectations' do; end
    end

    context 's3 file to local dir' do
      before do
        expect(filesystem).to receive(:s3cmd).with('get', 's3://bucket/old_file', new_dir)
        expect(filesystem).to receive(:s3cmd).with('del', 's3://bucket/old_file')
        instance.move_file_to_dir('s3://bucket/old_file', new_dir)
      end

      it 'meets expectations' do; end
    end

    context 's3 file to hdfs file' do
      before do
        expect(filesystem).to receive(:hadoop_fs).with('-mkdir', '-p', 'file://' + new_dir)
        expect(filesystem).to receive(:hadoop_fs).with('-mv', 's3n://bucket/old_file', 'file://' + new_dir)
        instance.move_file_to_dir('s3://bucket/old_file', 'file://' + new_dir)
      end

      it 'meets expectations' do; end
    end
  end

  describe '#move_dir' do
    subject(:removes_old_dir) do
      !File.exists?(old_dir)
    end

    subject(:creates_new_dir) do
      File.exists?(new_dir)
    end

    context 'local dir to local dir' do
      before do
        instance.move_dir(old_dir, new_dir)
      end

      it { expect(removes_old_dir).to eq(true) }
      it { expect(creates_new_dir).to eq(true) }
    end

    context 'local dir to s3 dir' do
      before do
        expect(filesystem).to receive(:s3cmd).with('put', '--recursive', old_dir + '/', 's3://bucket/new_dir/')
        instance.move_dir(old_dir, 's3://bucket/new_dir')
      end

      it 'meets expectations' do; end
      it { expect(removes_old_dir).to eq(true) }
    end

    context 'local dir to hdfs dir' do
      before do
        instance.move_dir(old_dir, 'file://' + new_dir)
      end

      it { expect(removes_old_dir).to eq(true) }
      it { expect(creates_new_dir).to eq(true) }
    end

    context 'hdfs dir to hdfs dir' do
      before do
        instance.move_dir('file://' + old_dir, 'file://' + new_dir)
      end

      it { expect(removes_old_dir).to eq(true) }
      it { expect(creates_new_dir).to eq(true) }
    end

    context 'hdfs dir to local dir' do
      before do
        instance.move_dir('file://' + old_dir, new_dir)
      end

      it { expect(removes_old_dir).to eq(true) }
      it { expect(creates_new_dir).to eq(true) }
    end

    context 'hdfs dir to s3 dir' do
      before do
        expect(filesystem).to receive(:hadoop_fs).with('-cp', 'file://' + old_dir, 's3n://bucket/new_dir/')
        expect(filesystem).to receive(:hadoop_fs).with('-rmr', 'file://' + old_dir)
        instance.move_dir('file://' + old_dir, 's3://bucket/new_dir')
      end

      it 'meets expectations' do; end
    end

    context 's3 dir to s3 dir' do
      before do
        expect(filesystem).to receive(:s3cmd).with('mv', '--recursive', 's3://bucket/old_dir/', 's3://bucket/new_dir')
        instance.move_dir('s3://bucket/old_dir', 's3://bucket/new_dir')
      end

      it 'meets expectations' do; end
    end

    context 's3 dir to local dir' do
      before do
        expect(filesystem).to receive(:s3cmd).with('get', '--recursive', 's3://bucket/old_dir/', new_dir)
        expect(filesystem).to receive(:s3cmd).with('del', '--recursive', 's3://bucket/old_dir/')
        expect(filesystem).to receive(:s3cmd).with('del', '--recursive', 's3://bucket/old_dir_$folder$')
        instance.move_dir('s3://bucket/old_dir', new_dir)
      end

      it 'meets expectations' do; end
    end

    context 's3 dir to hdfs dir' do
      before do
        expect(filesystem).to receive(:hadoop_fs).with('-mkdir', '-p', 'file://' + new_dir)
        expect(filesystem).to receive(:hadoop_fs).with('-cp', 's3n://bucket/old_dir', 'file://' + new_dir)
        expect(filesystem).to receive(:s3cmd).with('del', '--recursive', 's3://bucket/old_dir/')
        expect(filesystem).to receive(:s3cmd).with('del', '--recursive', 's3://bucket/old_dir_$folder$')
        instance.move_dir('s3://bucket/old_dir', 'file://' + new_dir)
      end

      it 'meets expectations' do; end
    end
  end

  context 'directory marked as immutable' do
    let(:dir) { 's3://bucket/incoming' }
    let(:file) { File.join(dir, '20130420.log') }

    before do
      instance.add_path(:incoming, dir, immutable: true)
    end

    describe '#remove_dir' do
      subject do
        instance.remove_dir(dir)
      end

      it { expect { subject }.to raise_error RuntimeError, /#{dir} is marked as immutable, cannot modify #{dir}/ }

      context 'nested directory' do
        let(:nested_dir) { File.join(dir, '2013') }

        subject do
          instance.remove_dir(nested_dir)
        end

        it { expect { subject }.to raise_error RuntimeError, /#{dir} is marked as immutable, cannot modify #{nested_dir}/ }
      end
    end

    describe '#move_file_to_file' do
      subject do
        instance.move_file_to_file(file, 's3://bucket/processed/new_file')
      end
      it { expect { subject }.to raise_error RuntimeError, /#{dir} is marked as immutable, cannot modify #{file}/ }
    end

    describe '#move_file_to_dir' do
      subject do
        instance.move_file_to_dir(dir, 's3://bucket/processed')
      end

      it { expect { subject }.to raise_error RuntimeError, /#{dir} is marked as immutable, cannot modify #{dir}/ }
    end

    describe '#move_dir' do
      subject do
        instance.move_dir(dir, 's3://bucket/processed/')
      end

      it { expect { subject }.to raise_error RuntimeError, /#{dir} is marked as immutable, cannot modify #{dir}/ }
    end
  end

  describe '#cat' do
    context 'simple file' do
      before do
        instance.write('dog', new_file)
      end

      subject do
        instance.cat(new_file).string
      end

      it { is_expected.to eq('dog') }
    end

    context 'result of directory glob' do
      before do
        instance.add_path(:new_dir, new_dir)
        instance.write('dog', instance.path(:new_dir, 'a', 'b', 'c', 'dog'))
      end

      subject do
        instance.cat(*instance.glob(instance.path(:new_dir, '*'))).string
      end

      it { is_expected.to eq('dog') }
    end
  end

  describe '#chown!' do
    context 'local' do
      subject(:operation) do
        instance.chown!(old_file)
      end
      it { expect { operation }.to_not raise_error }
    end

    context 'hdfs' do
      subject(:operation) do
        instance.chown!('file://' + old_file)
      end
      it { expect { operation }.to_not raise_error }
    end
  end

  describe '#glob_to_regexp' do
    let(:recursive) { false }
    subject(:file_regexp) { instance.glob_to_regexp(path, recursive: recursive) }

    context 'a path without glob' do
      let(:path) { '/tmp' }
      it { is_expected.to eq(%r{\A/tmp\z}) }
    end

    context 'a path without glob with recursive' do
      let(:recursive) { true }
      let(:path) { '/tmp' }
      it { is_expected.to eq(%r{\A/tmp}) }
    end

    context 'a path with glob' do
      let(:path) { '/tmp/*' }
      it { is_expected.to eq(%r{\A/tmp/?.*?}) }
    end

    context 'a path with glob with recursive' do
      let(:recursive) { true }
      let(:path) { '/tmp/*' }
      it { is_expected.to eq(%r{\A/tmp/?.*?}) }
    end
  end
end

describe Masamune::Filesystem do
  let(:instance) { filesystem }

  it_behaves_like 'Filesystem'
end

describe Masamune::CachedFilesystem do
  let(:instance) { described_class.new(filesystem) }

  it_behaves_like 'Filesystem'
end
