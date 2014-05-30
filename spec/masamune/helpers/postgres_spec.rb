require 'spec_helper'

describe Masamune::Helpers::Postgres do
  let(:context) { double }
  let(:instance) { described_class.new(context) }

  describe '.table_exists' do
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
end
