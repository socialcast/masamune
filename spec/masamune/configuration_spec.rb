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

  describe '#bind_template' do
    let(:section) { nil }
    let(:template) { nil }
    let(:params) { nil }

    subject do
      instance.bind_template(section, template, params)
    end

    context 'with invalid template section' do
      let(:section) { :missing_section }
      it { expect { subject }.to raise_error(ArgumentError) }
    end

    context 'when template section is missing' do
      let(:section) { :elastic_mapreduce }
      it { expect { subject }.to raise_error(ArgumentError) }
    end

    context 'with valid template section' do
      let(:section) { :elastic_mapreduce }
      before do
        instance.elastic_mapreduce[:templates] = {
          list_with_state: {
            command: '--list --key-pair %key_pair --start %state --verbose=%verbose',
            default: {
              state: 'RUNNING',
              verbose: true
            }
          },
          broken_template: nil
        }
      end

      context 'when template is missing' do
        let(:template) { :missing_template }
        it { expect { subject }.to raise_error(ArgumentError, 'no template for missing_template') }
      end

      context 'when template is broken' do
        let(:template) { :broken_template }
        it { expect { subject }.to raise_error(ArgumentError, 'no template for broken_template') }
      end

      context 'when params missing but default exists' do
        let(:template) { :list_with_state }
        let(:params) { {key_pair: 'emr-2013'} }
        it { is_expected.to eq(['--list', '--key-pair', 'emr-2013', '--start', 'RUNNING', '--verbose=true']) }
      end

      context 'when params missing and default missing' do
        let(:template) { :list_with_state }
        let(:params) { {state: 'COMPLETED'} }
        it { expect { subject }.to raise_error(ArgumentError, 'no param for %key_pair') }
      end

      context 'with params' do
        let(:template) { :list_with_state }
        let(:params) { {key_pair: 'emr-2013', state: 'COMPLETED'} }
        it { is_expected.to eq(['--list', '--key-pair', 'emr-2013', '--start', 'COMPLETED', '--verbose=true']) }
      end
    end
  end
end
