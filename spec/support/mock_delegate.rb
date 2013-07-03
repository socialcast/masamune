class MockDelegate
  attr_accessor :command, :stdout, :stderr

  def initialize(command)
    self.command  = command
    self.stdout   = []
    self.stderr   = []
  end

  def command_args
    command
  end

  def handle_stdout(line, line_no)
    stdout[line_no] = line
  end

  def handle_stderr(line, line_no)
    stderr[line_no] = line
  end
end

