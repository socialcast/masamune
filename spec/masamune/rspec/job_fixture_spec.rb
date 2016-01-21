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

describe Masamune::JobFixture do
  let!(:tmp_dir) { File.join(Dir.tmpdir, SecureRandom.hex) }
  let(:fixture_path) { File.join(tmp_dir, 'example', 'spec') }

  describe '.file_name' do
    subject { described_class.file_name(options) }
    context 'with file' do
      let(:options) { { file: File.join(fixture_path, 'job_fixture.yml') } }
      it { is_expected.to eq(File.join(fixture_path, 'job_fixture.yml')) }
    end

    context 'with path' do
      let(:options) { { path: fixture_path } }
      it { is_expected.to eq(File.join(fixture_path, 'job_fixture.yml')) }
    end

    context 'with path and type' do
      let(:options) { { path: fixture_path, type: 'task' } }
      it { is_expected.to eq(File.join(fixture_path, 'task_fixture.yml')) }
    end

    context 'with path and name' do
      let(:options) { { path: fixture_path, name: 'basic' } }
      it { is_expected.to eq(File.join(fixture_path, 'basic.job_fixture.yml')) }
    end
  end

  describe '#save' do
    let(:instance) { described_class.new(path: fixture_path, data: data) }
    let(:data) do
      {
        'inputs' => [
          {
            'file' => 'input_file',
            'data' => 'input_data'
          },
          {
            'file' => 'another_input_file',
            'data' => <<-EOS.strip_heredoc
              more_data
              more_data
            EOS
          },
          {
            'reference' => {
              'fixture' => 'other',
              'section' => 'output'
            }
          }
        ],
        'outputs' => [
          {
            'file' => 'output_file',
            'data' => 'output_data'
          },
          {
            'file' => 'another_output_file',
            'data' => <<-EOS.strip_heredoc
              more_data
              more_data
            EOS
          }
        ]
      }
    end

    before do
      instance.save
    end

    subject { File.read(instance.file_name) }

    it 'saves pretty fixture' do
      is_expected.to eq <<-EOS.strip_heredoc
        ---
        inputs:
          -
            file: input_file
            data: input_data
          -
            file: another_input_file
            data: |
              more_data
              more_data
          -
            reference:
              fixture: other
              section: output

        outputs:
          -
            file: output_file
            data: output_data
          -
            file: another_output_file
            data: |
              more_data
              more_data
      EOS
    end
  end

  describe '.load' do
    let(:basic_data) do
      {
        'inputs' => [
          {
            'file' => 'basic_input_file',
            'data' => 'basic_input_data'
          }
        ],
        'outputs' => [
          {
            'file' => 'basic_output_file',
            'data' => 'basic_output_data'
          }
        ]
      }
    end

    let(:reference_data) { {} }
    let(:another_reference_data) { {} }

    before do
      described_class.new(path: fixture_path, name: 'basic', data: basic_data).save
      described_class.new(path: fixture_path, name: 'reference', data: reference_data).save
      described_class.new(path: fixture_path, name: 'another_reference', data: another_reference_data).save
    end

    context 'with basic fixture from path' do
      subject(:instance) { described_class.load(path: fixture_path, name: 'basic') }

      it 'loads basic fixture' do
        expect(instance.inputs).to eq(basic_data['inputs'])
        expect(instance.outputs).to eq(basic_data['outputs'])
      end
    end

    context 'with basic fixture from file' do
      subject(:instance) { described_class.load(file: File.join(fixture_path, 'basic.job_fixture.yml')) }

      it 'loads basic fixture' do
        expect(instance.inputs).to eq(basic_data['inputs'])
        expect(instance.outputs).to eq(basic_data['outputs'])
      end
    end

    context 'with basic fixture from path that does not exist' do
      subject(:instance) { described_class.load(path: fixture_path, name: 'unknown') }

      it { expect { instance }.to raise_error(ArgumentError) }
    end

    context 'with basic fixture from file that does not exist' do
      subject(:instance) { described_class.load(file: File.join(fixture_path, 'unknown.job_fixture.yml')) }

      it { expect { instance }.to raise_error(ArgumentError) }
    end

    context 'with reference fixture' do
      let(:reference_data) do
        {
          'inputs' => [
            {
              'file' => 'other_input_file',
              'data' => 'other_input_data'
            },
            {
              'reference' => {
                'fixture' => 'basic'
              }
            }
          ],
          'outputs' => [
            {
              'file' => 'other_output_file',
              'data' => 'other_output_data'
            }
          ]
        }
      end

      subject(:instance) { described_class.load(path: fixture_path, name: 'reference') }

      it 'loads referenced fixture' do
        expect(instance.inputs).to include(reference_data['inputs'].first)
        expect(instance.inputs).to include(basic_data['outputs'].first)
        expect(instance.outputs).to eq(reference_data['outputs'])
      end
    end

    context 'with reference fixture and path' do
      let(:reference_data) do
        {
          'inputs' => [
            {
              'file' => 'other_input_file',
              'data' => 'other_input_data'
            },
            {
              'reference' => {
                'path' => fixture_path,
                'fixture' => 'basic'
              }
            }
          ],
          'outputs' => [
            {
              'file' => 'other_output_file',
              'data' => 'other_output_data'
            }
          ]
        }
      end

      subject(:instance) { described_class.load(path: fixture_path, name: 'reference') }

      it 'loads referenced fixture' do
        expect(instance.inputs).to include(reference_data['inputs'].first)
        expect(instance.inputs).to include(basic_data['outputs'].first)
        expect(instance.outputs).to eq(reference_data['outputs'])
      end
    end

    context 'with reference fixture from file' do
      let(:reference_data) do
        {
          'inputs' => [
            {
              'file' => 'other_input_file',
              'data' => 'other_input_data'
            },
            {
              'reference' => {
                'file' => File.join(fixture_path, 'basic.job_fixture.yml')
              }
            }
          ],
          'outputs' => [
            {
              'file' => 'other_output_file',
              'data' => 'other_output_data'
            }
          ]
        }
      end

      subject(:instance) { described_class.load(path: fixture_path, name: 'reference') }

      it 'loads referenced fixture' do
        expect(instance.inputs).to include(reference_data['inputs'].first)
        expect(instance.inputs).to include(basic_data['outputs'].first)
        expect(instance.outputs).to eq(reference_data['outputs'])
      end
    end


    context 'with reference fixture and section' do
      let(:reference_data) do
        {
          'inputs' => [
            {
              'file' => 'other_input_file',
              'data' => 'other_input_data'
            },
            {
              'reference' => {
                'fixture' => 'basic',
                'section' => 'inputs'
              }
            }
          ],
          'outputs' => [
            {
              'file' => 'other_output_file',
              'data' => 'other_output_data'
            }
          ]
        }
      end

      subject(:instance) { described_class.load(path: fixture_path, name: 'reference') }

      it 'loads referenced fixture' do
        expect(instance.inputs).to include(reference_data['inputs'].first)
        expect(instance.inputs).to include(basic_data['inputs'].first)
        expect(instance.outputs).to eq(reference_data['outputs'])
      end
    end

    context 'with reference fixture that does not exist' do
      let(:reference_data) do
        {
          'inputs' => [
            {
              'file' => 'other_input_file',
              'data' => 'other_input_data'
            },
            {
              'reference' => {
                'fixture' => 'unknown'
              }
            }
          ],
          'outputs' => [
            {
              'file' => 'other_output_file',
              'data' => 'other_output_data'
            }
          ]
        }
      end

      subject(:instance) { described_class.load(path: fixture_path, name: 'reference') }

      it { expect { instance.inputs }.to raise_error(ArgumentError) }
    end

    context 'with invalid reference fixture' do
      let(:reference_data) do
        {
          'inputs' => [
            {
              'file' => 'other_input_file',
              'data' => 'other_input_data'
            },
            {
              'reference' => 'unknown'
            }
          ],
          'outputs' => [
            {
              'file' => 'other_output_file',
              'data' => 'other_output_data'
            }
          ]
        }
      end

      subject(:instance) { described_class.load(path: fixture_path, name: 'reference') }

      it { expect { instance.inputs }.to raise_error(ArgumentError) }
    end

    context 'with reference fixture that includes reference' do
      let(:another_reference_data) do
        {
          'inputs' => [
            {
              'file' => 'another_input_file',
              'data' => 'another_input_data'
            },
            {
              'reference' => {
                'fixture' => 'basic',
                'section' => 'inputs'
              }
            }
          ],
          'outputs' => [
            {
              'file' => 'another_output_file',
              'data' => 'another_output_data'
            }
          ]
        }
      end

      let(:reference_data) do
        {
          'inputs' => [
            {
              'file' => 'other_input_file',
              'data' => 'other_input_data'
            },
            {
              'reference' => {
                'fixture' => 'another_reference',
                'section' => 'inputs'
              }
            }
          ],
          'outputs' => [
            {
              'file' => 'other_output_file',
              'data' => 'other_output_data'
            }
          ]
        }
      end

      subject(:instance) { described_class.load(path: fixture_path, name: 'reference') }

      it 'loads both referenced fixtures' do
        expect(instance.inputs).to include(reference_data['inputs'].first)
        expect(instance.inputs).to include(basic_data['inputs'].first)
        expect(instance.inputs).to include(another_reference_data['inputs'].first)
        expect(instance.outputs).to eq(reference_data['outputs'])
      end
    end
  end
end
