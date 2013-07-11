require 'spec_helper'

describe Masamune::Actions::Streaming do
  include Masamune::Actions::Streaming

  before do
    mock_command(/\Ahadoop/, mock_success)
  end

  subject { streaming }

  it { should be_success }
end
