require 'spec_helper'

describe Masamune::Helpers::Postgres do
  let(:environment) { double }
  let(:instance) { described_class.new(environment) }

  describe '#table_exists' do
    before do
      expect(instance).to receive(:database_exists?).and_return(true)
      expect(instance).to receive(:postgres).with(hash_including(:exec, :tuple_output)).and_yield('  foo').and_yield('  bar').and_yield('  baz')
    end

    subject { instance.table_exists?(table) }

    context 'when table exists' do
      let(:table) { 'foo' }
      it { is_expected.to eq(true) }
    end

    context 'when other table exists' do
      let(:table) { 'baz' }
      it { is_expected.to eq(true) }
    end

    context 'when table does not exist' do
      let(:table) { 'zombo' }
      it { is_expected.to eq(false) }
    end
  end

  describe '#table_last_modified_at' do
    subject { instance.table_last_modified_at('foo', options) }

    context 'with last_modified_at option' do
      before do
        expect(instance).to receive(:table_exists?).and_return(true)
        expect(instance).to receive(:postgres).with(hash_including(:exec, :tuple_output)).and_yield(output)
      end

      let(:options) { { last_modified_at: 'last_modified_at' } }

      context 'with expected output' do
        let(:output) { '  2014-06-04 10:20:19.539656-07' }

        it { is_expected.to be_a(Time) }
        it { is_expected.to eq(Time.parse('2014-06-04 17:20:00 +0000')) }
      end

      context 'with blank output' do
        let(:output) { '  ' }

        it { is_expected.to be_nil }
      end

      context 'with invalid output' do
        let(:output) { '  2XXX' }

        it { is_expected.to be_nil }
      end
    end

    context 'without last_modified_at option' do
      let(:options) { {} }

      before do
        expect(instance).to receive(:table_exists?).never
        expect(instance).to receive(:postgres).never
      end

      it { is_expected.to be_nil }
    end
  end
end
