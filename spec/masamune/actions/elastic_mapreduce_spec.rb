require 'spec_helper'

describe Masamune::Actions::ElasticMapreduce do
  include Masamune::Actions::ElasticMapreduce

  let(:var_dir) { Dir.mktmpdir('masamune') }

  before do
    Masamune.configuration.filesystem.add_path(:var_dir, var_dir)
    mock_command(/\Aelastic-mapreduce/, mock_success)
  end

  subject { elastic_mapreduce }

  it { should be_success }
end
