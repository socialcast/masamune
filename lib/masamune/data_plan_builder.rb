class Masamune::DataPlanBuilder
  class << self
    def build_via_thor(namespaces, commands, sources, targets)
      Masamune::DataPlan.new.tap do |data_plan|
        sources_for, sources_anon = partition_by_for(sources)
        targets_for, targets_anon = partition_by_for(targets)

        commands.each do |name, command|
          command_name = "#{namespaces.shift}:#{name}"

          source = sources_for[name] || sources_anon.shift or next
          target = targets_for[name] || targets_anon.shift or next

          source_name, source_options = source
          target_name, target_options = target

          data_plan.add_source_rule(command_name, source_name, source_options)
          data_plan.add_target_rule(command_name, target_name, target_options)

          data_plan.add_command_rule(command_name, command_wrapper(command_name))
        end
      end
    end

    private

    def partition_by_for(annotations)
      with_for, anon = annotations.partition { |_, opts| opts.has_key?(:for) }
      decl = {}
      with_for.each do |name, opts|
        decl[opts[:for]] = [name, opts.reject { |k,_| k == :for }]
      end
      [decl, anon]
    end

    def command_wrapper(command_name)
      Proc.new do |plan, rule, runtime_options|
        $THOR_MASTER.invoke(rule) unless $THOR_MASTER.current_command_name == rule
      end
    end
  end
end
