Masamune
========
> The first of the swords was by all accounts a fine sword, however it is a blood thirsty, evil blade, as it does not discriminate as to who or what it will cut. It may just as well be cutting down butterflies as severing heads. The second was by far the finer of the two, as it does not needlessly cut that which is innocent and undeserving.


Description
------------
Masamune provides a [dataflow programming](http://en.wikipedia.org/wiki/Dataflow_programming) framework on top of [Thor](http://whatisthor.com/). In the framework, dataflows are constructed as Thor tasks that transform source data into the target data. Source and target data descriptions are encoded as annotations associated with the Thor command. From these source and target annotations, Masamune constructs a data dependency tree that describes how to automatically construct a target data set.

Usage
----------

Describe your dataflow as source, target data transformations:
```ruby
class ExampleThor  < Thor
  # Mix in Masamune specific Data Flow Behavior
  include Masamune::Thor
  include Masamune::Actions::DataFlow

  # Mix in Masamune Actions for Data Processing
  include Masamune::Actions::Streaming
  include Masamune::Actions::Hive

  # Describe a Data Processing Job
  desc 'extract_logs', 'Organize log files by YYYY-MM-DD'

  target fs.path(:target_dir, '%Y-%m-%d', mkdir: true)
  source fs.path(:source_dir, '%Y%m%d*.log')
  def extract_logs
    targets.missing.each do |target|
      target.sources.each do |source|
        # Transform source into target
        fs.copy(source.path, target.path)
      end
    end
  end
end
```

Execute your dataflow with the goal of processing all data from the start of the year:

```
thor extract_logs --start '1 year ago'
```

Contributing
---------------

* Fork the project
* Fix the issue
* Add unit tests
* Submit pull request on github
