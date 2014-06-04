require 'spec_helper'

describe Masamune::Helpers::Postgres do
  let(:context) { double }
  let(:instance) { described_class.new(context) }

  describe '#table_exists' do
    before do
      instance.should_receive(:postgres).with(hash_including(:exec, :fail_fast)).and_return(mock_success)
      instance.should_receive(:postgres).with(hash_including(:exec, :tuple_output)).and_yield('  foo').and_yield('  bar').and_yield('  baz')
    end

    subject { instance.table_exists?(table) }

    context 'when table exists' do
      let(:table) { 'foo' }
      it { should be_true }
    end

    context 'when other table exists' do
      let(:table) { 'baz' }
      it { should be_true }
    end

    context 'when table does not exist' do
      let(:table) { 'zombo' }
      it { should be_false }
    end
  end

  describe '#last_modified_at' do
    before do
      instance.should_receive(:postgres).with(hash_including(:exec, :tuple_output)).and_yield(output)
    end

    subject { instance.last_modified_at('foo') }

    context 'with expected output' do
      let(:output) { '  2014-06-04 09:15:52.027382' }

      it { should be_a(DateTime) }
      it { should == DateTime.civil(2014,6,4,9,15) }
    end

    context 'with blank output' do
      let(:output) { '  ' }

      it { should be_nil }
    end

    context 'with invalid output' do
      let(:output) { '  2XXX' }

      it { should be_nil }
    end
  end

  describe '#truncate_table' do
    before do
      instance.should_receive(:postgres).with(exec: 'TRUNCATE TABLE foo;', fail_fast: true).and_return(mock_success)
      instance.truncate_table('foo')
    end


    it 'meets expectations' do; end
  end
end
