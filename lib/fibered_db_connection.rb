module ArFibers
  module FiberedDBConnection

    # Attaches the connection socket to an event loop and adds a callback
    # to the fiber's callbacks that unregisters the connection from event loop
    # Raises NB::NBError
    def register_with_event_loop
      if EM.reactor_running?
        if @fiber.nil? && @em_connection.nil?
          # This connection hasn't been visited by a fiber recently and
          # it's not attached to the event loop
          @fiber = Fiber.current
          @em_connection = EM::watch(socket,EMConnectionHandler,self) { |c| c.notify_readable = true }
          raise "No :callbacks on fiber ??" unless @fiber[:callbacks]
          @fiber[:callbacks] << self.method(:unregister_from_event_loop)
        elsif @em_connection
          # This connection has been visited by a fiber before and it already
          # set the callbacks to unregister it. just update the fiber. Either
          # a new fiber is trying to use the connection or the first one wich
          # has the callbacks. The case of a new fiber can happen when
          # ConnectionPool#process_queue is called where a fiber resumes
          # another fiber
          @fiber = Fiber.current
        else
          # Something has gone wrong !
          raise ArFibers::Error.new("FiberedDBConnection: Something has gone wrong !")
        end       
      else
        raise ArFibers::Error.new("FiberedDBConnection: EventMachine reactor not running")
      end
    end  

    # Unattaches the connection socket from the event loop
    def unregister_from_event_loop
      if @em_connection
        @em_connection.detach
        @em_connection = nil
        @fiber = nil
        true
      else
        false
      end
    end

    # Removes the unregister_from_event_loop callback from the fiber's
    # callbacks. It should be used when errors occur in an already registered
    # connection
    def remove_unregister_from_event_loop_callbacks
      @fiber[:callbacks].delete self.method(:unregister_from_event_loop)
    end

    # Closes the connection using event loop
    def event_loop_connection_close
      @em_connection.close_connection if @em_connection
    end
         
    # The event loop callback, this is called whenever there is data
    # available at the socket
    def resume_command
      @fiber.resume if @fiber
    end
  end
  
  module EMConnectionHandler
    def initialize connection
      @db_connection = connection
    end
    def notify_readable
      @db_connection.resume_command
    end
  end
end


