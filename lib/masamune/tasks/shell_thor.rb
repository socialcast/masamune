require 'masamune'
require 'thor'
require 'pry'

module Masamune::Tasks
  class ShellThor < Thor
    include Masamune::Thor
    include Masamune::Actions::DataFlow
    include Masamune::Transform::DefineSchema

    # FIXME need to add an unnecessary namespace until this issue is fixed:
    # https://github.com/wycats/thor/pull/247
    namespace :shell

    desc 'shell', 'Launch an interactive shell'
    method_option :dump, :type => :boolean, :desc => 'Dump SQL schema', :default => false
    method_option :type, :enum => ['psql', 'hql'], :desc => 'Schema type', :default => 'psql'
    method_option :prompt, :desc => 'Set shell prompt', :default => 'masamune'
    class_option :start, :aliases => '-a', :desc => 'Start time', default: '1 month ago'
    def shell_exec
      if options[:dump]
        print_registry
        exit
      end

      Pry.start self, prompt: proc { options[:prompt] + '> ' }
    end
    default_task :shell_exec

    private

    def print_registry
      case options[:type]
      when 'psql'
        puts define_schema(registry, :postgres)
      when 'hql'
        puts define_schema(registry, :hive)
      end
    end
  end
end
