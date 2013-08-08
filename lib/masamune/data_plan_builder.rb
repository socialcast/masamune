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

    def command_options(runtime_options)
      runtime_options.reject { |_,v| v == false }.map { |k,v| ["--#{k}", v == true ? nil : v] }.flatten.compact
    end

    # TODO try using Thor::invoke
    def command_wrapper(command_name)
      Proc.new do |sources, runtime_options|
        command_options = command_options(runtime_options)
        Masamune.logger.debug([command_name, '--sources', save_paths_to_file(sources)] + command_options)
        namespace, task = command_name.split(':')
        thor = Thor::Util.find_by_namespace(namespace)
        thor.start([task, '--sources', save_paths_to_file(sources)] + command_options)
      end
    end

    def save_paths_to_file(*paths)
      Tempfile.new('masamune').tap do |file|
        file.write(paths.join("\n"))
        file.close
      end.path
    end
  end
end
