require 'spec_helper'

describe Masamune::Actions::Streaming do
  include Masamune::Actions::Streaming

  let(:var_dir) { Dir.mktmpdir('masamune') }

  before do
    Masamune.configuration.filesystem.add_path(:var_dir, var_dir)
    mock_command(/\Ahadoop/, mock_success)
  end

  subject { streaming }

  it { should be_success }
end
