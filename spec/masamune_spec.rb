require 'spec_helper'

describe Masamune do
  it { should be_a(Module) }
  its(:context) { should be_a(Masamune::Context) }
end
