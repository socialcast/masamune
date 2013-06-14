require 'spec_helper'

describe Masamune::Configuration do
  let(:client) { Masamune::Client.new }
  let(:instance) { described_class.new(client) }

  describe '#hive' do
    subject { instance.hive }

    it { should == {:database => 'default', :options => []} }

    context 'after adding a symbol key' do
      before do
        instance.hive[:random] = '123'
      end
      it { should == {:database => 'default', :options => [], :random => '123' } }
    end

    context 'after adding a string key' do
      before do
        instance.hive['random'] = '123'
      end
      it { should == {:database => 'default', :options => [], :random => '123' } }
    end

    context 'after adding a override key' do
      before do
        instance.hive['database'] = 'test'
      end
      it { should == {:database => 'test', :options => []} }
    end
  end

  describe '#hadoop_streaming' do
    subject { instance.hadoop_streaming }

    it { should == {:jar => instance.default_hadoop_streaming_jar, :options => []} }
  end

  describe '#hadoop_filesystem' do
    subject { instance.hadoop_filesystem }

    it { should == {:options => []} }
  end

  describe '#elastic_mapreduce' do
    subject { instance.elastic_mapreduce }

    it { should == {:enabled => false, :options => []} }
  end

  describe '#s3cmd' do
    subject { instance.s3cmd}

    it { should == {:options => []} }
  end
end
