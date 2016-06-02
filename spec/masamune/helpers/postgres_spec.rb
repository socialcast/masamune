#  The MIT License (MIT)
#
#  Copyright (c) 2014-2016, VMware, Inc. All Rights Reserved.
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

describe Masamune::Helpers::Postgres do
  let(:environment) { double }
  let(:instance) { described_class.new(environment) }

  describe '#database_exists' do
    let(:mock_status) {}

    before do
      expect(instance).to receive(:postgres).with(hash_including(exec: 'SELECT version();', fail_fast: false, retries: 0)).and_return(mock_status)
    end

    subject { instance.database_exists? }

    context 'when database exists' do
      let(:mock_status) { mock_success }
      it { is_expected.to eq(true) }
    end

    context 'when database does not exist' do
      let(:mock_status) { mock_failure }
      it { is_expected.to eq(false) }
    end
  end

  describe '#table_exists' do
    before do
      expect(instance).to receive(:database_exists?).and_return(true)
      expect(instance).to receive(:postgres).with(hash_including(exec: 'SELECT table_name FROM information_schema.tables;', tuple_output: true, retries: 0)).and_yield('  foo').and_yield('  bar').and_yield('  baz')
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
        expect(instance).to receive(:postgres).with(hash_including(exec: 'SELECT MAX(last_modified_at) FROM foo;', tuple_output: true, retries: 0)).and_yield(output).and_yield('')
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
