require 'spec_helper'

describe Masamune::DataPlanBuilder do
  describe '.build_via_thor' do
    subject(:data_plan) do
      described_class.build_via_thor(namespaces, commands, sources, targets)
    end

    context 'with multiple namespaces' do
      let(:namespaces) { ['a', 'b'] }
      let(:commands) { {'load' => mock, 'store' => mock} }
      let(:sources) { [['log/%Y%m%d.*.log', {}], ['table/y=%Y/m=%m/d=%d', {}]] }
      let(:targets) { [['table/y=%Y/m=%m/d=%d', {}], ['daily/%Y-%m-%d', {}]] }

      before do
        Masamune::DataPlan.any_instance.should_receive(:add_target_rule).with('a:load', 'table/y=%Y/m=%m/d=%d', {})
        Masamune::DataPlan.any_instance.should_receive(:add_source_rule).with('a:load', 'log/%Y%m%d.*.log', {})
        Masamune::DataPlan.any_instance.should_receive(:add_command_rule).with('a:load', an_instance_of(Proc))
        Masamune::DataPlan.any_instance.should_receive(:add_target_rule).with('b:store', 'daily/%Y-%m-%d', {})
        Masamune::DataPlan.any_instance.should_receive(:add_source_rule).with('b:store', 'table/y=%Y/m=%m/d=%d', {})
        Masamune::DataPlan.any_instance.should_receive(:add_command_rule).with('b:store', an_instance_of(Proc))
        subject
      end

      it 'should build a Masamune::DataPlan instance' do; end
    end

    context 'with :for option' do
      let(:namespaces) { ['a', 'a', 'a'] }
      let(:commands) { {'missing_before' => mock, 'override' => mock, 'missing_after' => mock} }
      let(:sources) { [['log/%Y%m%d.*.log', {for: 'override'}]] }
      let(:targets) { [['table/y=%Y/m=%m/d=%d', {for: 'override'}]] }

      before do
        Masamune::DataPlan.any_instance.should_receive(:add_target_rule).with('a:override', 'table/y=%Y/m=%m/d=%d', {})
        Masamune::DataPlan.any_instance.should_receive(:add_source_rule).with('a:override', 'log/%Y%m%d.*.log', {})
        Masamune::DataPlan.any_instance.should_receive(:add_command_rule).with('a:override', an_instance_of(Proc))
        subject
      end

      it 'should build a Masamune::DataPlan instance' do; end
    end
  end
end
