# Silence noisy and uninformative create_command method
module Masamune::ThorMute
  def self.included(base)
    base.instance_eval do
      def create_command(*a)
        $stdout, tmp_stdout = StringIO.new, $stdout
        super *a
      ensure
        $stdout = tmp_stdout
      end
    end
  end
end
