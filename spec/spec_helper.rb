require 'bundler/setup'
Bundler.require(:default, :development, :test)

require 'tempfile'
require 'tmpdir'

require 'masamune/spec_helper'

RSpec.configure do |config|
  config.mock_with :rspec
end
