class IO
  def wait_readable(timeout = 0)
    rs, ws = IO.select([self], [], [], timeout)
    rs && rs[0]
  end unless IO.method_defined?(:wait_readable)

  def wait_writable(timeout = 0)
    rs, ws = IO.select([], [self], [], timeout)
    ws && ws[0]
  end unless IO.method_defined?(:wait_writable)
end
