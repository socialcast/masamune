require 'spec_helper'

describe Masamune do
  it { should be_a(Module) }
  its(:default_context) { should be_a(Masamune::Context) }
  its(:default_config_file) { should =~ %r{config/masamune\.yml\.erb\Z} }
end
