require 'spec_helper'

describe Masamune::Actions::Postgres do
  include Masamune::Actions::Postgres

  before do
    mock_command(/\Apsql/, mock_success)
  end

  subject { postgres }

  it { should be_success }
end
