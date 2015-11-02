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

describe Masamune::StringFormat do
  let(:instance) { Object.new.extend(described_class) }

  describe '.strip_sql' do
    subject { instance.strip_sql(input) }

    context 'with quoted sql' do
      let(:input) { %q('SELECT * FROM table;') }
      it { is_expected.to eq(%q(SELECT * FROM table;)) }
    end

    context 'with ; terminated sql' do
      let(:input) { %q(SELECT * FROM table;;) }
      it { is_expected.to eq(%q(SELECT * FROM table;)) }
    end

    context 'with multi line sql' do
      let(:input) do
        <<-EOS
            SELECT
              *
            FROM
              table
            ;

        EOS
      end
      it { is_expected.to eq(%q(SELECT * FROM table;)) }
    end

    context 'with un-quoted sql' do
      let(:input) { %q(SELECT * FROM table) }
      it { is_expected.to eq(%q(SELECT * FROM table;)) }
    end
  end
end
