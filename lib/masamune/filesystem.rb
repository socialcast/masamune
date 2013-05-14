class Masamune::Filesystem
  extend Forwardable

  def initialize
    @locations = {}
  end

  def add_location(symbol, value)
    @locations[symbol] = value
  end

  def_delegators :@locations, :[]
end
