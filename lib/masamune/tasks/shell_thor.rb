require 'masamune'
require 'thor'
require 'pry'

module Masamune::Tasks
  class ShellThor < Thor
    include Masamune::Thor
    include Masamune::Actions::DataFlow

    # FIXME need to add an unnecessary namespace until this issue is fixed:
    # https://github.com/wycats/thor/pull/247
    namespace :shell

    desc 'shell', 'Launch an interactive shell'
    def shell_exec
      Pry.start self, prompt: proc { 'masamune> ' }
    end
    default_task :shell_exec
  end
end
