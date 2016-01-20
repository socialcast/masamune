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

describe Masamune::Schema::Store do
  let(:environment) { double }

  context 'without type' do
    subject(:store) { described_class.new(environment) }
    it { expect { store }.to raise_error ArgumentError, 'required parameter type: missing' }
  end

  context 'with type :unknown' do
    subject(:store) { described_class.new(environment, type: :unknown) }
    it { expect { store }.to raise_error ArgumentError, "unknown type: 'unknown'" }
  end

  context 'with type :postgres' do
    subject(:store) { described_class.new(environment, type: :postgres) }
    it { expect(store.format).to eq(:csv) }
    it { expect(store.headers).to be_truthy }
    it { expect(store.json_encoding).to eq(:quoted) }

    context 'with format override' do
      subject(:store) { described_class.new(environment, type: :postgres, format: :raw) }
      it { expect(store.format).to eq(:raw) }
      it { expect(store.headers).to be_falsey }
      it { expect(store.json_encoding).to eq(:raw) }
    end
  end

  context 'with type :hive' do
    subject(:store) { described_class.new(environment, type: :hive) }
    it { expect(store.format).to eq(:tsv) }
    it { expect(store.headers).to be_falsey }
    it { expect(store.json_encoding).to eq(:raw) }
  end

  context 'with type :files' do
    subject(:store) { described_class.new(environment, type: :files) }
    it { expect(store.format).to eq(:raw) }
    it { expect(store.headers).to be_falsey }
    it { expect(store.json_encoding).to eq(:raw) }

    context 'with format overrides' do
      subject(:store) { described_class.new(environment, type: :files, format: :csv, headers: true, json_encoding: :quoted) }
      it { expect(store.format).to eq(:csv) }
      it { expect(store.headers).to be_truthy }
      it { expect(store.json_encoding).to eq(:quoted) }
    end
  end
end
