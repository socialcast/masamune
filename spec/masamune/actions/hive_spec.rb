require 'spec_helper'

describe Masamune::Actions::Hive do
  include Masamune::Actions::Hive

  let(:var_dir) { Dir.mktmpdir('masamune') }

  before do
    Masamune.configuration.filesystem.add_path(:var_dir, var_dir)
    mock_command(/\Ahive/, mock_success)
  end

  subject { hive }

  it { should be_success }
end
