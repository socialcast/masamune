require 'spec_helper'

describe Masamune::DataPlanBuilder do
  describe '#build' do
    subject(:data_plan) do
      described_class.instance.build(namespaces, commands, sources, targets)
    end

    context 'with multiple namespaces' do
      let(:namespaces) { ['a', 'b'] }
      let(:commands) { ['load', 'store'] }
      let(:sources) { [{ path: 'log/%Y%m%d.*.log' }, { path: 'table/y=%Y/m=%m/d=%d' }] }
      let(:targets) { [{ path: 'table/y=%Y/m=%m/d=%d' }, { path: 'daily/%Y-%m-%d' }] }

      before do
        Masamune::DataPlan.any_instance.should_receive(:add_target_rule).with('a:load', { path: 'table/y=%Y/m=%m/d=%d'})
        Masamune::DataPlan.any_instance.should_receive(:add_source_rule).with('a:load', { path: 'log/%Y%m%d.*.log' })
        Masamune::DataPlan.any_instance.should_receive(:add_command_rule).with('a:load', an_instance_of(Proc))
        Masamune::DataPlan.any_instance.should_receive(:add_target_rule).with('b:store', { path: 'daily/%Y-%m-%d' })
        Masamune::DataPlan.any_instance.should_receive(:add_source_rule).with('b:store', { path: 'table/y=%Y/m=%m/d=%d' })
        Masamune::DataPlan.any_instance.should_receive(:add_command_rule).with('b:store', an_instance_of(Proc))
        subject
      end

      it 'should build a Masamune::DataPlan instance' do; end
    end

    context 'with :for option' do
      let(:namespaces) { ['a', 'a', 'a'] }
      let(:commands) { ['missing_before', 'override', 'missing_after'] }
      let(:sources) { [{ path: 'log/%Y%m%d.*.log', for: 'override' }] }
      let(:targets) { [{ path: 'table/y=%Y/m=%m/d=%d', for: 'override' }] }

      before do
        Masamune::DataPlan.any_instance.should_not_receive(:add_target_rule).with('a:missing_before', anything)
        Masamune::DataPlan.any_instance.should_not_receive(:add_source_rule).with('a:missing_before', anything)
        Masamune::DataPlan.any_instance.should_not_receive(:add_command_rule).with('a:missing_before', an_instance_of(Proc))
        Masamune::DataPlan.any_instance.should_receive(:add_target_rule).with('a:override', { path: 'table/y=%Y/m=%m/d=%d' })
        Masamune::DataPlan.any_instance.should_receive(:add_source_rule).with('a:override', { path: 'log/%Y%m%d.*.log' })
        Masamune::DataPlan.any_instance.should_receive(:add_command_rule).with('a:override', an_instance_of(Proc))
        Masamune::DataPlan.any_instance.should_not_receive(:add_target_rule).with('a:missing_after', anything)
        Masamune::DataPlan.any_instance.should_not_receive(:add_source_rule).with('a:missing_after', anything)
        Masamune::DataPlan.any_instance.should_not_receive(:add_command_rule).with('a:missing_after', an_instance_of(Proc))
        subject
      end

      it 'should build a Masamune::DataPlan instance' do; end
    end

    context 'with :skip option' do
      let(:namespaces) { ['a', 'a', 'a'] }
      let(:commands) { ['missing_before', 'override'] }
      let(:sources) { [{ skip: true}, { path: 'log/%Y%m%d.*.log' }] }
      let(:targets) { [{ skip: true}, { path: 'table/y=%Y/m=%m/d=%d' }] }

      before do
        Masamune::DataPlan.any_instance.should_not_receive(:add_target_rule).with('a:missing_before', anything)
        Masamune::DataPlan.any_instance.should_not_receive(:add_source_rule).with('a:missing_before', anything)
        Masamune::DataPlan.any_instance.should_not_receive(:add_command_rule).with('a:missing_before', an_instance_of(Proc))
        Masamune::DataPlan.any_instance.should_receive(:add_target_rule).with('a:override', { path: 'table/y=%Y/m=%m/d=%d' })
        Masamune::DataPlan.any_instance.should_receive(:add_source_rule).with('a:override', { path: 'log/%Y%m%d.*.log' })
        Masamune::DataPlan.any_instance.should_receive(:add_command_rule).with('a:override', an_instance_of(Proc))
        subject
      end

      it 'should build a Masamune::DataPlan instance' do; end
    end
  end
end
