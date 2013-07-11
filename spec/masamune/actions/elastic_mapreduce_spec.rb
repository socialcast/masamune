require 'spec_helper'

describe Masamune::Actions::ElasticMapreduce do
  include Masamune::Actions::ElasticMapreduce

  before do
    mock_command(/\Aelastic-mapreduce/, mock_success)
  end

  subject { elastic_mapreduce }

  it { should be_success }
end
