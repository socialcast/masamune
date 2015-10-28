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

describe Masamune::SharedExampleGroup do
  it { is_expected.to be_a(Module) }

  let(:klass) { Class.new.send(:include, described_class) }

  describe '.example_fixture_file' do
    before do
      allow(klass).to receive(:file_path).and_return(file_path)
    end

    subject { klass.example_fixture_file(options) }

    context 'with file_path like task_spec.rb' do
      let(:file_path) { './examples/apache_log/spec/task_spec.rb' }

      context 'with options empty' do
        let(:options) { {} }
        it { is_expected.to eq('./examples/apache_log/spec/task_fixture.yml') }
      end

      context 'with options fixture:' do
        let(:options) { { fixture: 'fixture_name' } }
        it { is_expected.to eq('./examples/apache_log/spec/fixture_name.task_fixture.yml') }
      end

      context 'with options file:' do
        let(:options) { { file: './examples/apache_log/spec/task_fixture.yml' } }
        it { is_expected.to eq('./examples/apache_log/spec/task_fixture.yml') }
      end
    end

    context 'with file_path like mapper_spec.rb' do
      let(:file_path) { './examples/apache_log/spec/mapper_spec.rb' }

      context 'with options empty' do
        let(:options) { {} }
        it { is_expected.to eq('./examples/apache_log/spec/mapper_fixture.yml') }
      end

      context 'with options fixture:' do
        let(:options) { { fixture: 'fixture_name' } }
        it { is_expected.to eq('./examples/apache_log/spec/fixture_name.mapper_fixture.yml') }
      end

      context 'with options file:' do
        let(:options) { { file: './examples/apache_log/spec/mapper_fixture.yml' } }
        it { is_expected.to eq('./examples/apache_log/spec/mapper_fixture.yml') }
      end
    end
  end
end
