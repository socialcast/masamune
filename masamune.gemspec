$:.push File.expand_path('../lib', __FILE__)

require 'masamune/version'

Gem::Specification.new do |s|
  s.name        = 'masamune'
  s.version     = Masamune::VERSION
  s.authors     = ['Michael Andrews']
  s.email       = ['michael@socialcast.com']
  s.homepage    = 'https://github.com/socialcast/masamune'
  s.summary     = 'Hybrid Data & Work Flow'
  s.description = 'Hybrid Data & Work Flow'

  s.files = Dir['{bin,lib,spec/support/masamune}/**/*'] + ['LICENSE', 'Rakefile', 'README.md']
  s.test_files = Dir['spec/**/*']
  s.require_path = 'lib'
  s.executables = ['masamune-hive', 'masamune-elastic-mapreduce', 'masamune-psql']

  s.add_dependency('thor')
  s.add_dependency('activesupport')
  s.add_dependency('tzinfo')
  s.add_dependency('chronic')
  s.add_dependency('tilt')
  s.add_dependency('erubis')

  # Development
  s.add_development_dependency('rake', '>= 0.9')

  # Testing
  s.add_development_dependency('rspec', '>= 2.12')
  s.add_development_dependency('debugger')
end
