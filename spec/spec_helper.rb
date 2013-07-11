require 'bundler/setup'
Bundler.require(:default, :development, :test)

require 'tempfile'
require 'tmpdir'

Dir["#{File.dirname(__FILE__)}/support/**/*.rb"].each {|f| require f}

RSpec.configure do |config|
  config.mock_with :rspec
end
