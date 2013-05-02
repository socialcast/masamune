class Fixpoint::DataPlan
  DEFAULT_HANDLER = Proc.new {}

  class Binding < Array
    def targets
      map(&:first)
    end
    def sources
      map(&:last)
    end
  end

  def initialize(fs, handler = DEFAULT_HANDLER)
    @fs = fs
    @rules = []
    @handler = handler
  end

  def add_rule(rule, template, &block)
    @rules << [Fixpoint::Matcher.new(rule), [template, block.to_proc]]
  end

  def resolve(start, stop)
    (start .. stop).each do |interval|
      batch_commands = Hash.new { |h,k| h[k] = Binding.new }
      batch_handlers = Hash.new { |h,k| h[k] = Binding.new }

      @rules.each do |rule, (template, command)|
        target = rule.bind_date(interval)
        source = rule.bind(target, template)
        unless @fs.exists?(target)
          if @fs.exists?(source)
            batch_commands[command] << [target, source]
          else
            batch_handlers[rule] << [target, source]
          end
        end
      end

      batch_commands.each do |command, bindings|
        command.call bindings
      end

      batch_handlers.each do |rule, bindings|
        @handler.call bindings
      end
    end
  end
end
