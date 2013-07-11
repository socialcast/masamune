require 'spec_helper'

describe Masamune::Actions::S3Cmd do
  include Masamune::Actions::S3Cmd

  let(:var_dir) { Dir.mktmpdir('masamune') }

  before do
    Masamune.configuration.filesystem.add_path(:var_dir, var_dir)
    mock_command(/\As3cmd/, mock_success)
  end

  subject { s3cmd 'ls', 's3://fake-bucket' }

  it { should be_success }
end
