module ArFibers

  #	A pool of initialized fibers
  #	It does not grow in size or create transient fibers
  #	It will queue code blocks when needed (if all its fibers are busy)
  #
  # This class is particulary useful when you use the fibers 
  # to connect to evented back ends. It also does not generate
  # transient objects and thus saves memory.
  #
  # Example:
  # fiber_pool = NeverBlock::Pool::FiberPool.new(150)
  # 
  # loop do
  #   fiber_pool.spawn do
  #     #fiber body goes here 
  #   end
  # end
  #
  class FiberPool

    # gives access to the currently free fibers
    attr_reader :fibers

    # Prepare a list of fibers that are able to run different blocks of code
    # every time. Once a fiber is done with its block, it attempts to fetch
    # another one from the queue
    def initialize(count = 50)
      @fibers,@busy_fibers,@queue = [],{},[]
      count.times do |i|
        create_fiber
      end
    end
    
    
    def create_fiber
      fiber = Fiber.new do |block|
        loop do
          block.call
          # callbacks are called in a reverse order, much like c++ destructor
          Fiber.current[:callbacks].pop.call while Fiber.current[:callbacks].length > 0
          unless @queue.empty?
            block = @queue.shift
          else
            @busy_fibers.delete(Fiber.current.object_id)
            @fibers << Fiber.current
            block = Fiber.yield
          end
        end
      end
      fiber[:callbacks] = []
      fiber[:em_keys] = []
      fiber[:neverblock] = true
      @fibers << fiber
    end
    
    # If there is an available fiber use it, otherwise, leave it to linger
    # in a queue
    # if a fiber is found dead, remove it from the pool and replace it
    #
    def spawn(evented = true, &block)
      begin
        if fiber = @fibers.shift
          fiber[:callbacks] = []
          @busy_fibers[fiber.object_id] = fiber
          fiber[:neverblock] = evented
          fiber.resume(block)
        else
          @queue << block
        end
      rescue FiberError
        # if a fiber died
        unless fiber.alive?
          @busy_fibers.delete(fiber.object_id)
          # create a new one to replace it so we never run out of fibers
          create_fiber()
          # and retry
          retry
        end
      end

      self # we are keen on hiding our queue
    end

  end # FiberPool
end # ArFibers

