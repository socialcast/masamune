class Masamune::MockDelegate
  include Masamune::HasContext

  attr_accessor :command, :stdin, :stdout, :stderr, :status

  def initialize(command, input = nil)
    self.command  = command
    self.stdin    = StringIO.new(input) if input
    self.stdout   = []
    self.stderr   = []
    self.status   = 0
  end

  def command_args
    [command]
  end

  def handle_stdout(line, line_no)
    self.stdout[line_no] = line
  end

  def handle_stderr(line, line_no)
    self.stderr[line_no] = line
  end

  def handle_failure(code)
    self.status = code
  end
end
