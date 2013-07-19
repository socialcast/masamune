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

    context 'overriding path with valid path symbol' do
      before do
        instance.hive = { path: 'whoami' }
      end
      it { should == {:path => 'whoami', :database => 'default', :options => []} }
    end

    context 'overriding path with valid path string' do
      before do
        instance.hive = { 'path' => 'whoami' }
      end
      it { should == {:path => 'whoami', :database => 'default', :options => []} }
    end

    context 'overriding path with non resolvable path' do
      subject do
        instance.hive = { path: 'whoami_' }
      end
      it { expect { subject }.to raise_error Thor::InvocationError, 'Invalid path whoami_ for command hive' }
    end

    context 'overriding path an absolute path' do
      before do
        instance.hive = {path: '/usr/bin/whoami'}
      end
      it { should == {path: '/usr/bin/whoami', database: 'default', options: []} }
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

  describe '#jobflow=' do
    before do
      instance.elastic_mapreduce[:enabled] = true
    end

    subject do
      instance.jobflow
    end
    it { should be_nil }

    context 'with jobflow value' do
      before do
        instance.jobflow = 'j-value'
      end
      it { should == 'j-value' }

      context 'when elastic_mapreduce[:enabled] is false' do
        before do
          instance.elastic_mapreduce[:enabled] = false
          instance.jobflow = 'j-value'
        end
        it { should be_nil }
      end
    end

    context 'with jobflow symbol when configured jobflows not defined' do
      before do
        instance.elastic_mapreduce[:jobflows] = nil
        instance.jobflow = 'j-value'
      end
      it { should == 'j-value' }
    end

    context 'with jobflow symbol when configured jobflows defined' do
      before do
        instance.elastic_mapreduce[:jobflows] = {build: 'j-build'}
        instance.jobflow = 'build'
      end
      it { should == 'j-build' }
    end
  end

  describe '#bind_template' do
    let(:section) { nil }
    let(:template) { nil }
    let(:params) { nil }

    subject do
      instance.bind_template(section, template, params)
    end

    context 'with invalid template section' do
      let(:section) { :missing_section }
      it { expect { subject }.to raise_error(ArgumentError) }
    end

    context 'when template section is missing' do
      let(:section) { :elastic_mapreduce }
      it { expect { subject }.to raise_error(ArgumentError) }
    end

    context 'with valid template section' do
      let(:section) { :elastic_mapreduce }
      before do
        instance.elastic_mapreduce[:templates] = {
          list_with_state: {
            command: '--list --key-pair %key_pair --start %state',
            default: {
              state: 'RUNNING'
            }
          },
          broken_template: nil
        }
      end

      context 'when template is missing' do
        let(:template) { :missing_template }
        it { expect { subject }.to raise_error(ArgumentError) }
      end

      context 'when template is broken' do
        let(:template) { :broken_template }
        it { expect { subject }.to raise_error(ArgumentError) }
      end

      context 'when params missing but default exists' do
        let(:template) { :list_with_state }
        let(:params) { {key_pair: 'emr-2013'} }
        it { should == ['--list', '--key-pair', 'emr-2013', '--start', 'RUNNING'] }
      end

      context 'when params missing and default missing' do
        let(:template) { :list_with_state }
        let(:params) { {state: 'COMPLETED'} }
        it { expect { subject }.to raise_error(ArgumentError) }
      end

      context 'with params' do
        let(:template) { :list_with_state }
        let(:params) { {key_pair: 'emr-2013', state: 'COMPLETED'} }
        it { should == ['--list', '--key-pair', 'emr-2013', '--start', 'COMPLETED'] }
      end
    end
  end
end
