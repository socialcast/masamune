module Masamune
  module Actions
    class << self
      def after_register(&block)
        @after_register ||= []
        if block_given?
          @after_register << block
        else
          @after_register
        end
      end
    end

    require 'masamune/actions/hive'
    require 'masamune/actions/s3cmd'
    require 'masamune/actions/streaming'
    require 'masamune/actions/postgres'
    require 'masamune/actions/postgres_admin'
    require 'masamune/actions/filesystem'
    require 'masamune/actions/data_flow'
    require 'masamune/actions/elastic_mapreduce'
    require 'masamune/actions/execute'
  end
end
