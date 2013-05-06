class Masamune::DataPlan
  def initialize
    @rules = []
    @matches = Hash.new { |h,k| h[k] = [] }
  end

  def add_rule(rule, template, command, &block)
    @rules << [Masamune::Matcher.new(rule), [template, command, block.to_proc]]
  end

  def resolve(start, stop)
    (start .. stop).each do |interval|
      @rules.each do |rule, (template, command, filter)|
        target = rule.bind_date(interval)
        source = rule.bind(target, template)
        if !filter.call(target) && filter.call(source)
          @matches[command] << source
        end
      end
    end
  end

  def matches
    @matches
  end
end
