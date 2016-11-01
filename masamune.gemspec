$LOAD_PATH.push File.expand_path('../lib', __FILE__)

require 'masamune/version'

Gem::Specification.new do |s|
  s.name        = 'masamune'
  s.version     = Masamune::VERSION
  s.authors     = ['Michael Andrews']
  s.email       = ['michael@socialcast.com']
  s.homepage    = 'https://github.com/socialcast/masamune'
  s.summary     = 'Hybrid Data & Work Flow'
  s.description = 'Hybrid Data & Work Flow'

  s.files = Dir['{bin,lib,spec/support/masamune}/**/*'] + ['LICENSE.txt', 'Rakefile', 'README.md']
  s.test_files = Dir['spec/**/*']
  s.require_path = 'lib'
  s.executables = ['masamune-shell', 'masamune-hive', 'masamune-aws-emr', 'masamune-psql', 'masamune-dump']

  s.add_dependency('thor')
  s.add_dependency('activesupport')
  s.add_dependency('tzinfo')
  s.add_dependency('chronic')
  s.add_dependency('tilt')
  s.add_dependency('erubis')
  s.add_dependency('parallel')
  s.add_dependency('pry')
  # Needed to work around: https://github.com/pry/pry/issues/1217
  s.add_dependency('rb-readline')
  s.add_dependency('hashie')

  # Development
  s.add_development_dependency('rake', '>= 0.9')
  s.add_development_dependency('rubocop')
  s.add_development_dependency('user_agent_parser')

  # Testing
  s.add_development_dependency('rspec', '> 2.99')
  s.add_development_dependency('debugger') if RUBY_VERSION.start_with?('1')
  s.add_development_dependency('byebug') if RUBY_VERSION.start_with?('2')
end
