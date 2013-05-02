$:.push File.expand_path('../lib', __FILE__)

require 'fixpoint/version'

# NOTE alternative names
# point break
# data break
# fixpoint
# becoming
# emerging
# emergent
Gem::Specification.new do |s|
  s.name        = 'fixpoint'
  s.version     = Fixpoint::VERSION
  s.authors     = ['Michael Andrews']
  s.email       = ['michael@socialcast.com']
  s.homepage    = 'https://github.com/socialcast/becoming'
  s.summary     = 'Hybrid Data & Work Flow'
  s.description = 'Hybrid Data & Work Flow- meant to converage'

  s.files = Dir['lib/**/*'] + ['LICENSE', 'Rakefile', 'README.md']
  s.test_files = Dir['spec/**/*']

  # Development
  s.add_development_dependency('rake', '~> 0.9')

  # Testing
  s.add_development_dependency('rspec', '~> 2.12')
  s.add_development_dependency('debugger')
end
