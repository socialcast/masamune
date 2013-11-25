require 'spec_helper'

describe Masamune::Configuration do
  let(:context) { Masamune::Context.new }
  let(:instance) { described_class.new(context) }

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
        it { should == ['--list', '--key-pair', 'emr-2013', '--start', 'RUNNING', '--verbose=true'] }
      end

      context 'when params missing and default missing' do
        let(:template) { :list_with_state }
        let(:params) { {state: 'COMPLETED'} }
        it { expect { subject }.to raise_error(ArgumentError, 'no param for %key_pair') }
      end

      context 'with params' do
        let(:template) { :list_with_state }
        let(:params) { {key_pair: 'emr-2013', state: 'COMPLETED'} }
        it { should == ['--list', '--key-pair', 'emr-2013', '--start', 'COMPLETED', '--verbose=true'] }
      end
    end
  end
end
