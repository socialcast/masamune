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

describe Masamune::Configuration do
  let(:environment) { Masamune::Environment.new }
  let(:instance) { described_class.new(environment) }

  describe '.default_config_file' do
    subject { described_class.default_config_file }
    it { is_expected.to match(%r{config/masamune\.yml\.erb\Z}) }
  end

  describe '#default_config_file' do
    subject { instance.default_config_file }
    it { is_expected.to match(%r{config/masamune\.yml\.erb\Z}) }
  end

  describe '#as_options' do
    subject { instance.as_options }
    it { is_expected.to eq([]) }

    context 'with dry_run: true and debug: true' do
      before do
        instance.debug = instance.dry_run = true
      end
      it { is_expected.to eq(['--debug', '--dry-run']) }
    end
  end
end
