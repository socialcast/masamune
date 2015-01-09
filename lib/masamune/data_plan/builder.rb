require 'singleton'

class Masamune::DataPlan::Builder
  include Singleton

  def build(namespaces, commands, sources, targets)
    Masamune::DataPlan::Engine.new.tap do |data_plan|
      sources_for, sources_anon = partition_by_for(sources)
      targets_for, targets_anon = partition_by_for(targets)

      commands.each do |name|
        command_name = "#{namespaces.shift}:#{name}"

        source_options = sources_for[name] || sources_anon.shift or next
        target_options = targets_for[name] || targets_anon.shift or next
        next if source_options[:skip] || target_options[:skip]

        data_plan.add_source_rule(command_name, source_options)
        data_plan.add_target_rule(command_name, target_options)

        data_plan.add_command_rule(command_name, thor_command_wrapper)
      end
    end
  end

  private

  def partition_by_for(annotations)
    with_for, anon = annotations.partition { |opts| opts.has_key?(:for) }
    decl = {}
    with_for.each do |opts|
      decl[opts[:for]] = opts.reject { |k,_| k == :for }
    end
    [decl, anon]
  end

  def thor_command_wrapper
    Proc.new do |plan, rule, _|
      plan.environment.with_exclusive_lock(rule) do
        plan.environment.parent.invoke(rule)
      end
    end
  end
end
