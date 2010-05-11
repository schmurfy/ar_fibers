# First remove the current connection pool which assumes threads

ActiveRecord::ConnectionAdapters.send :remove_const, :ConnectionPool

# Now replace it with our own version that assumes it's just a single thread
# but with many fibers

class ActiveRecord::ConnectionAdapters::ConnectionPool
  attr_reader :spec

  def initialize(spec)
    @spec = spec
    @fiber_queue = []
    # The cache of reserved connections mapped to threads
    @reserved_connections = {}

    # default 5 second timeout unless on ruby 1.9
    @timeout =
      if RUBY_VERSION < '1.9'
        spec.config[:wait_timeout] || 5
      end

    # default max pool size to 5
    @size = (spec.config[:pool] && spec.config[:pool].to_i) || 5

    @connections = []
    @checked_out = []
  end

  # Retrieve the connection associated with the current fiber, or call
  # #checkout to obtain one if necessary.
  #
  # #connection can be called any number of times; the connection is
  # held in a hash keyed by the thread id.
  def connection
    if conn = @reserved_connections[current_connection_id]
      conn
    else
      @reserved_connections[current_connection_id] = checkout
    end
  end

  # Signal that the thread is finished with the current connection.
  # #release_connection releases the connection-thread association
  # and returns the connection to the pool.
  def release_connection
    conn = @reserved_connections.delete(current_connection_id)
    checkin conn if conn
  end

  # Reserve a connection, and yield it to a block. Ensure the connection is
  # checked back in when finished.
  def with_connection
    conn = checkout
    yield conn
  ensure
    checkin conn
  end

  # Returns true if a connection has already been opened.
  def connected?
    !@connections.empty?
  end

  # Disconnects all connections in the pool, and clears the pool.
  def disconnect!
    @reserved_connections.each do |name,conn|
      checkin conn
    end
    @reserved_connections = {}
    @connections.each do |conn|
      conn.disconnect!
    end
    @connections = []
  end

  # Clears the cache which maps classes
  def clear_reloadable_connections!
    @reserved_connections.each do |name, conn|
      checkin conn
    end
    @reserved_connections = {}
    @connections.each do |conn|
      conn.disconnect! if conn.requires_reloading?
    end
    @connections = []
  end

  # Verify active connections and remove and disconnect connections
  # associated with stale threads.
  def verify_active_connections! #:nodoc:
    clear_stale_cached_connections!
    @connections.each do |connection|
      connection.verify!
    end
  end

  def clear_stale_cached_connections!
    #do nothing !
    #TODO see how this can be done for dead fibers - if any
    #remove_stale_cached_threads!(cache, &block)
  end

  def checkout
    # puts "[#{Fiber.current}] Checkout"
    conn = if @checked_out.size < @connections.size
             checkout_existing_connection
           elsif @connections.size < @size
             checkout_new_connection
           else
             Fiber.yield @fiber_queue << Fiber.current             
           end
    Fiber.current[:callbacks] ||= []
    Fiber.current[:callbacks] << self.method(:process_queue)
    Fiber.current[:current_pool_key] = current_pool_id
    # puts "[#{Fiber.current}] Checkout returned #{conn}"
    conn
  end

  def checkin(conn)
    # puts "[#{Fiber.current}] Checkin #{conn}"
    conn.run_callbacks :checkin
    @checked_out.delete conn
  end

  private

  # Check if there are waiting fibers and
  # try to process them
	def process_queue    
		while !@fiber_queue.empty? && @checked_out.size < @connections.size
			fiber = @fiber_queue.shift
			# What is really happening here?
			# we are resuming a fiber from within
			# another, should we call transfer instead?
      fiber.resume checkout_existing_connection
		end
	end

  def new_connection
    ActiveRecord::Base.send(spec.adapter_method, spec.config)
  end

  def current_connection_id #:nodoc:
    Fiber.current.object_id
  end

  def current_pool_id
    "cp_#{object_id}"
  end

  #TODO see how this can be done for dead fibers - if any
#  def remove_stale_cached_threads!(cache, &block)
#    keys = Set.new(cache.keys)
#
#    Thread.list.each do |thread|
#      keys.delete(thread.object_id) if thread.alive?
#    end
#    keys.each do |key|
#      next unless cache.has_key?(key)
#      block.call(key, cache[key])
#      cache.delete(key)
#    end
#  end

  def checkout_new_connection
    c = new_connection
    @connections << c
    checkout_and_verify(c)
  end

  def checkout_existing_connection
    c = (@connections - @checked_out).first
    checkout_and_verify(c)
  end

  def checkout_and_verify(c)
    c.verify!
    c.run_callbacks :checkout
    @checked_out << c
    c
  end
end
