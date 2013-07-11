require 'spec_helper'

describe Masamune::Actions::S3Cmd do
  include Masamune::Actions::S3Cmd

  before do
    mock_command(/\As3cmd/, mock_success)
  end

  subject { s3cmd 'ls', 's3://fake-bucket' }

  it { should be_success }
end
