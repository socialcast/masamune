require 'spec_helper'

describe Masamune::Actions::Hive do
  include Masamune::Actions::Hive

  before do
    mock_command(/\Ahive/, mock_success)
  end

  subject { hive }

  it { should be_success }
end
