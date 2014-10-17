require 'spec_helper'

describe Masamune::Schema::Row do
  subject(:row) { described_class.new }
  it { expect(row).to_not be_nil }
end
