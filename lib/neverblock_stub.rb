#
# mimic neverblock to allow drop in replacement
#

module NB
  module Pool
    FiberPool = ArFibers::FiberPool
  end
end
