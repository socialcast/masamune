require 'bundler/setup'
Bundler.require(:default, :development, :test)

require 'tempfile'
require 'tmpdir'

require 'masamune/spec_helper'

MasamuneExampleGroup.configure do |config|
  config.quiet = ENV['MASAMUNE_DEBUG'] ? false : true
  config.debug = ENV['MASAMUNE_DEBUG'] ? true : false
end

RSpec.configure do |config|
  config.mock_with :rspec
end
