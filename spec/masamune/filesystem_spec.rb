require 'spec_helper'

require 'securerandom'

# NOTE when operating between hdfs and s3, hadoop fs requires s3n URI
# See: http://wiki.apache.org/hadoop/AmazonS3
shared_examples_for 'Filesystem' do
  let(:filesystem) { Masamune::Filesystem.new }

  let(:old_dir) { Dir.mktmpdir('masamune') }
  let(:new_dir) { File.join(Dir.tmpdir, SecureRandom.hex) }
  let(:new_file) { File.join(old_dir, SecureRandom.hex) }
  let(:old_file) {
    File.join(old_dir, SecureRandom.hex).tap do |file|
      FileUtils.touch file
    end
  }

  after do
    FileUtils.rmdir(old_dir)
    FileUtils.rmdir(new_dir)
  end

  describe '#get_path' do
    context 'after add_path is called' do
      before do
        instance.add_path(:home_dir, '/home')
      end
      it { instance.get_path(:home_dir).should == '/home' }

      context 'with extra directories' do
        it { instance.get_path(:home_dir, 'a', 'b', 'c').should == '/home/a/b/c' }
      end

      context 'with extra directories delimited by "/"' do
        it { instance.get_path(:home_dir, '/a/b', 'c').should == '/home/a/b/c' }
      end
    end

    context 'before add_path is called' do
      it { instance.get_path(:home_dir).should be_a(Proc) }
    end
  end

  describe '#resolve_file' do
    subject { instance.resolve_file(paths) }

    context 'with nil' do
      let(:paths) { nil }
      it { should be_nil }
    end

    context 'with empty' do
      let(:paths) { [] }
      it { should be_nil }
    end

    context 'with one file' do
      let(:paths) { old_file }
      it { should == old_file }
    end

    context 'with directories and file' do
      let(:paths) { [old_dir, new_dir, new_file, old_file] }
      it { should == old_file }
    end
  end

  describe '#touch!' do
    subject do
      File.exists?(new_file)
    end

    context 'local' do
      before do
        instance.touch!(new_file)
      end
      it { should be_true }
    end

    context 'hdfs' do
      before do
        instance.touch!('file://' + new_file)
      end
      it { should be_true }
    end
  end

  describe '#exists?' do
    context 'local missing file' do
      subject { instance.exists?(new_file) }
      it { should be_false }
    end

    context 'hdfs missing file' do
      subject { instance.exists?('file://' + new_file) }
      it { should be_false }
    end

    context 'local existing file' do
      subject { instance.exists?(old_file) }
      it { should be_true }
    end

    context 'hdfs existing file' do
      subject { instance.exists?('file://' + old_file) }
      it { should be_true }
    end
  end

  describe '#mkdir!' do
    subject do
      Dir.exists?(new_dir)
    end

    context 'local directory' do
      before do
        instance.mkdir!(new_dir)
      end
      it { should be_true }
    end

    context 'hdfs directory' do
      before do
        instance.mkdir!('file://' + new_dir)
      end
      it { should be_true }
    end
  end

  describe '#glob' do
    subject do
      instance.glob(pattern)
    end

    context 'local no matches' do
      let(:pattern) { File.join(new_dir, '*') }
      it { should be_empty }
      it { expect { |b| instance.glob(pattern, &b) }.to_not yield_control }
    end

    context 'local one matches' do
      let(:pattern) { File.join(File.dirname(old_file), '*') }
      it { should_not be_empty }
      it { expect { |b| instance.glob(pattern, &b) }.to yield_with_args(old_file) }
    end

    context 'hdfs no matches' do
      let(:pattern) { File.join(new_dir, '*') }
      it { should be_empty }
      it { expect { |b| instance.glob('file://' + pattern, &b) }.to_not yield_control }
    end

    context 'hdfs one matches' do
      let(:pattern) { File.join(File.dirname(old_file), '*') }
      it { should_not be_empty }
      it { expect { |b| instance.glob('file://' + pattern, &b) }.to yield_with_args('file://' + old_file) }
    end

    context 's3 no matches' do
      let(:pattern) { 's3://bucket/dir/*.txt' }

      before do
        filesystem.should_receive(:s3cmd).with('ls', "s3://bucket/dir/*", safe: true)
      end

      it { should be_empty }
    end

    context 's3 one matches' do
      let(:pattern) { 's3://bucket/dir/*.txt' }

      before do
        filesystem.should_receive(:s3cmd).with('ls', "s3://bucket/dir/*", safe: true).
          and_yield(%q(2013-05-24 18:52      2912   s3://bucket/dir/file.txt)).
          and_yield(%q(2013-05-24 18:53      2912   s3://bucket/dir/file.csv))
      end

      it { should include 's3://bucket/dir/file.txt' }
      it { should_not include 's3://bucket/dir/file.csv' }
    end

    context 's3 many matches' do
      let(:pattern) { 's3://bucket/dir/*' }

      before do
        filesystem.should_receive(:s3cmd).with('ls', "s3://bucket/dir/*", safe: true).
          and_yield(%q(                       DIR   s3://bucket/dir/file_$folder$)).
          and_yield(%q(2013-05-24 18:52      2912   s3://bucket/dir/file.txt)).
          and_yield(%q(2013-05-24 18:53      2912   s3://bucket/dir/file.csv))
      end

      it { should include 's3://bucket/dir/file.txt' }
      it { should include 's3://bucket/dir/file.csv' }
    end
  end

  describe '#copy_file' do
    subject do
      File.exists?(File.join(new_dir, File.basename(old_file)))
    end

    context 'local file to local dir' do
      before do
        instance.copy_file(old_file, new_dir)
      end

      it { should be_true }
    end

    context 'local file to s3 dir' do
      before do
        filesystem.should_receive(:s3cmd).with('put', old_file, 's3://bucket/new_dir/')
        instance.copy_file(old_file, 's3://bucket/new_dir')
      end

      it 'meets expectations' do; end
    end

    context 'local file to hdfs dir' do
      before do
        instance.copy_file(old_file, 'file://' + new_dir)
      end

      it { should be_true }
    end

    context 'hdfs file to hdfs dir' do
      before do
        instance.copy_file('file://' + old_file, 'file://' + new_dir)
      end

      it { should be_true }
    end

    context 'hdfs file to local dir' do
      before do
        instance.copy_file('file://' + old_file, new_dir)
      end

      it { should be_true }
    end

    context 'hdfs file to s3 dir' do
      before do
        filesystem.should_receive(:execute_hadoop_fs).with('-cp', 'file://' + old_file, 's3n://bucket/new_dir')
        instance.copy_file('file://' + old_file, 's3://bucket/new_dir')
      end

      it 'meets expectations' do; end
    end

    context 's3 file to s3 dir' do
      before do
        filesystem.should_receive(:s3cmd).with('cp', 's3://bucket/old_file', 's3://bucket/new_dir/')
        instance.copy_file('s3://bucket/old_file', 's3://bucket/new_dir')
      end

      it 'meets expectations' do; end
    end

    context 's3 file to local dir' do
      before do
        filesystem.should_receive(:s3cmd).with('get', 's3://bucket/old_file', new_dir)
        instance.copy_file('s3://bucket/old_file', new_dir)
      end

      it 'meets expectations' do; end
    end

    context 's3 file to hdfs dir' do
      before do
        filesystem.should_receive(:execute_hadoop_fs).with('-mkdir', 'file://' + new_dir)
        filesystem.should_receive(:execute_hadoop_fs).with('-cp', 's3n://bucket/old_file', 'file://' + new_dir)
        instance.copy_file('s3://bucket/old_file', 'file://' + new_dir)
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
        instance.remove_dir(old_dir)
      end

      it { should be_false}
    end

    context 'hdfs dir' do
      before do
        instance.remove_dir('file://' + old_dir)
      end

      it { should be_false}
    end

    context 's3 dir' do
      before do
        filesystem.should_receive(:s3cmd).with('del', '--recursive', 's3://bucket/dir/')
        filesystem.should_receive(:s3cmd).with('del', '--recursive', 's3://bucket/dir_$folder$')
        instance.remove_dir('s3://bucket/dir')
      end

      it 'meets expectations' do; end
    end
  end

  describe '#move_file' do
    subject(:removes_old_file) do
      !File.exists?(old_file)
    end

    subject(:creates_new_file) do
      File.exists?(new_file)
    end

    context 'local file to local file' do
      before do
        instance.move_file(old_file, new_file)
      end

      it { removes_old_file.should be_true }
      it { creates_new_file.should be_true }
    end

    context 'local file to s3 file' do
      before do
        filesystem.should_receive(:s3cmd).with('put', old_file, 's3://bucket/new_dir')
        instance.move_file(old_file, 's3://bucket/new_dir')
      end

      it 'meets expectations' do; end
      it { removes_old_file.should be_true }
    end

    context 'local file to hdfs file' do
      before do
        instance.move_file(old_file, 'file://' + new_file)
      end

      it { removes_old_file.should be_true }
      it { creates_new_file.should be_true }
    end

    context 'hdfs file to hdfs file' do
      before do
        instance.move_file('file://' + old_file, 'file://' + new_file)
      end

      it { removes_old_file.should be_true }
      it { creates_new_file.should be_true }
    end

    context 'hdfs file to local file' do
      before do
        instance.move_file('file://' + old_file, new_file)
      end

      it { removes_old_file.should be_true }
      it { creates_new_file.should be_true }
    end

    context 'hdfs file to s3 file' do
      before do
        filesystem.should_receive(:execute_hadoop_fs).with('-mv', 'file://' + old_file, 's3n://bucket/new_dir')
        instance.move_file('file://' + old_file, 's3://bucket/new_dir')
      end

      it 'meets expectations' do; end
    end

    context 's3 file to s3 file' do
      before do
        filesystem.should_receive(:s3cmd).with('mv', 's3://bucket/old_file', 's3://bucket/new_dir')
        instance.move_file('s3://bucket/old_file', 's3://bucket/new_dir')
      end

      it 'meets expectations' do; end
    end

    context 's3 file to local file' do
      before do
        filesystem.should_receive(:s3cmd).with('get', 's3://bucket/old_file', new_dir)
        filesystem.should_receive(:s3cmd).with('del', 's3://bucket/old_file')
        instance.move_file('s3://bucket/old_file', new_dir)
      end

      it 'meets expectations' do; end
    end

    context 's3 file to hdfs file' do
      before do
        filesystem.should_receive(:execute_hadoop_fs).with('-mkdir', 'file://' + File.dirname(new_file))
        filesystem.should_receive(:execute_hadoop_fs).with('-mv', 's3n://bucket/old_file', 'file://' + new_file)
        instance.move_file('s3://bucket/old_file', 'file://' + new_file)
      end

      it 'meets expectations' do; end
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

      it { should == 'dog' }
    end

    context 'result of directory glob' do
      before do
        instance.add_path(:new_dir, new_dir)
        instance.write('dog', instance.path(:new_dir, 'a', 'b', 'c', 'dog'))
      end

      subject do
        instance.cat(*instance.glob(instance.path(:new_dir, '**', '*'))).string
      end

      it { should == 'dog' }
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
