class Masamune::MockDelegate
  attr_accessor :command, :stdout, :stderr, :status

  def initialize(command)
    self.command  = command
    self.stdout   = []
    self.stderr   = []
    self.status   = 0
  end

  def command_args
    command
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
