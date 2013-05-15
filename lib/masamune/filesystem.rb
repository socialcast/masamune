class Masamune::Filesystem
  extend Forwardable

  def initialize
    @locations = {}
  end

  def add_location(symbol, value, options = {})
    @locations[symbol] = value
    # TODO detect filesystem based on prefix
    if options[:mkdir]
      FileUtils.mkdir_p(value)
    end
  end

  def_delegators :@locations, :[]
end
