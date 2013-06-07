Masamune
========

Description
------------
Masamune provides a [dataflow programming](http://en.wikipedia.org/wiki/Dataflow_programming) framework on top of [Thor](http://whatisthor.com/). In the framework, dataflows are constructed as Thor tasks that transform  source data into the target data. Source and target data descriptions are encoded as annotations associated with the Thor command. From these source and target annotations, Masamune constructs a data dependency tree that describes how to automatically construct a target data set.  

Usage
----------

Describe your dataflow as source, target data transformations: 
```
class ExampleThor  < Thor
  # Mix in Masamune specific Data Flow Behavior
  include Masamune::Thor
  include Masamune::Actions::DataFlow
  
  # Mix in Masamune Actions for Data Processing
  include Masamune::Actions::Streaming
  include Masamune::Actions::Hive
  
  # Describe a Data Processing Job
  desc 'extract_logs', 'Organize log files by YYYY-MM-DD'
  
  target "#{fs.path(:target_dir)}/%Y-%m-%d", :for => :extract_logs
  source "#{fs.path(:source_dir)}/%Y%m%d*.log", :wildcard => true, :for => :extract_logs
  def extract_logs
    existing_sources.each do |source|
      # Transform source into target
      fs.copy(source.path, fs.path(:target_dir))
    end
  end
end
```

Execute your dataflow with the goal of processing all data from the start of the year:

```
thor extract_logs --start '2013-01-01'
```

