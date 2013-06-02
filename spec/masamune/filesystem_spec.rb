require 'spec_helper'

require 'tempfile'
require 'tmpdir'
require 'securerandom'

# TODO expect execute for hdfs
shared_examples_for 'Filesystem' do
  let(:instance) { Masamune::Filesystem.new }
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
    before do
      instance.add_path(:home_dir, '/home')
    end
    it { instance.get_path(:home_dir).should == '/home' }

    context 'with extra directories' do
      it { instance.get_path(:home_dir, 'a', 'b', 'c').should == '/home/a/b/c' }
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
      it { expect { |b| instance.glob('file://' + pattern, &b) }.to yield_with_args(old_file) }
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

    context 'hdfs file to hdfs dir' do
      before do
        instance.copy_file('file://' + old_file, 'file://' + new_dir)
      end

      it { should be_true }
    end

    context 's3 file to s3 dir' do
      pending 's3 test harness'
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
  end

  describe '#cat' do
    before do
      instance.write('dog', new_file)
    end

    subject do
      instance.cat(new_file).string
    end

    it { should == 'dog' }
  end
end

describe Masamune::Filesystem do
  it_behaves_like 'Filesystem'
end

describe Masamune::CachedFilesystem do
  it_behaves_like 'Filesystem'
end
