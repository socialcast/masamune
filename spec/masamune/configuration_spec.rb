require 'spec_helper'

describe Masamune::Configuration do
  let(:client) { Masamune::Client.new }
  let(:instance) { described_class.new(client) }

  describe '#hive' do
    subject { instance.hive }

    it { should == {:path => 'hive', :database => 'default', :options => []} }

    context 'after adding a symbol key' do
      before do
        instance.hive[:random] = '123'
      end
      it { should == {:path => 'hive', :database => 'default', :options => [], :random => '123' } }
    end

    context 'after adding a string key' do
      before do
        instance.hive['random'] = '123'
      end
      it { should == {:path => 'hive', :database => 'default', :options => [], :random => '123' } }
    end

    context 'after adding a override key' do
      before do
        instance.hive['database'] = 'test'
      end
      it { should == {:path => 'hive', :database => 'test', :options => []} }
    end
  end

  describe '#hadoop_streaming' do
    subject { instance.hadoop_streaming }

    it { should == {:path => 'hadoop', :jar => instance.default_hadoop_streaming_jar, :options => []} }
  end

  describe '#hadoop_filesystem' do
    subject { instance.hadoop_filesystem }

    it { should == {:path => 'hadoop', :options => []} }
  end

  describe '#elastic_mapreduce' do
    subject { instance.elastic_mapreduce }

    it { should == {:path => 'elastic-mapreduce', :enabled => false, :options => []} }
  end

  describe '#s3cmd' do
    subject { instance.s3cmd}

    it { should == {:path => 's3cmd', :options => []} }
  end

  describe '#hive=' do
    subject do
      instance.hive
    end

    context 'overriding existing options' do
      before do
        instance.hive['options'] = [{'-f' => 'flag'}]
        instance.hive = {:options => [{'-i' => 'first'}, {'-i' => 'last'}]}
      end
      it { should == {:path => 'hive', :database => 'default', :options => [{'-f' => 'flag'}, {'-i' => 'first'}, {'-i' => 'last'}]} }
    end

    context 'defining new options' do
      before do
        instance.hive['options'] = nil
        instance.hive = {:options => [{'-i' => 'first'}, {'-i' => 'last'}]}
      end
      it { should == {:path => 'hive', :database => 'default', :options => [{'-i' => 'first'}, {'-i' => 'last'}]} }
    end

    context 'preserving existing options' do
      before do
        instance.hive['options'] = [{'-f' => 'flag'}]
        instance.hive = {:database => 'test'}
      end
      it { should == {:path => 'hive', :database => 'test', :options => [{'-f' => 'flag'}]} }
    end
  end
end
