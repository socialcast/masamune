require 'spec_helper'

describe Masamune::Actions::S3Cmd do
  let(:klass) do
    Class.new do
      include Masamune::HasEnvironment
      include Masamune::Actions::S3Cmd
    end
  end

  let(:instance) { klass.new }

  describe '.s3cmd' do
    before do
      mock_command(/\As3cmd/, mock_success)
    end

    subject { instance.s3cmd 'ls', 's3://fake-bucket' }

    it { is_expected.to be_success }
  end
end
