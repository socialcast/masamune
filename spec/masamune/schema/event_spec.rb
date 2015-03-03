#  The MIT License (MIT)
#
#  Copyright (c) 2014-2015, VMware, Inc. All Rights Reserved.
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

require 'spec_helper'

describe Masamune::Schema::Event do
  context 'without id' do
    subject(:event) { described_class.new }
    it { expect { event }.to raise_error ArgumentError }
  end

  context 'with attributes' do
    let(:event) do
      described_class.new id: 'user',
        attributes: [
          Masamune::Schema::Event::Attribute.new(id: 'tenant_id', type: :integer),
          Masamune::Schema::Event::Attribute.new(id: 'user_id', type: :integer)
        ]
    end

    it { expect(event.attributes).to include :tenant_id }
    it { expect(event.attributes).to include :user_id }
    it { expect(event.attributes[:tenant_id].type).to eq(:integer) }
    it { expect(event.attributes[:user_id].type).to eq(:integer) }
  end

  context 'with array attributes' do
    let(:event) do
      described_class.new id: 'user',
        attributes: [
          Masamune::Schema::Event::Attribute.new(id: 'group_id', type: :integer, array: true),
        ]
    end

    it { expect(event.attributes).to include :group_id }
    it { expect(event.attributes[:group_id].type).to eq(:integer) }
    it { expect(event.attributes[:group_id].array).to be(true) }
  end


  describe Masamune::Schema::Event::Attribute do
    context 'without id' do
      subject(:attribute) { described_class.new }
      it { expect { attribute }.to raise_error ArgumentError }
    end

    subject(:attribute) { described_class.new id: 'id' }

    it do
      expect(attribute.id).to eq(:id)
      expect(attribute.type).to eq(:integer)
      expect(attribute.immutable).to eq(false)
      expect(attribute.array).to eq(false)
    end
  end
end
