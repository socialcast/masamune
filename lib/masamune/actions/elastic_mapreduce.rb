module Masamune::Actions
  module ElasticMapreduce
    include Masamune::Actions::Common

    def elastic_mapreduce_ssh(options = {})
      Masamune.logger.debug(options[:stdin])
      stdin =
        case options[:stdin]
        when IO
          options[:stdin]
        when String
          StringIO.new(options[:stdin])
        end
      execute('elastic-mapreduce', '--jobflow', Masamune.configuration.jobflow, '--ssh', :stdin => stdin)
    end
  end
end
