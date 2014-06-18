require 'masamune/has_environment'

# Separate environment for test harness itself
module MasamuneExampleGroup
  include Masamune::HasEnvironment
  extend self

  def self.included(base)
    base.before(:all) do
      self.filesystem.environment = self.environment = MasamuneExampleGroup.environment
      Thor.send(:include, Masamune::ThorMute)
    end
  end
end
