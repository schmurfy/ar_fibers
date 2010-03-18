require 'rubygems'
require 'ruby-debug'
# require 'mysqlplus'

class Thread
  def self.new(*args)
    raise
  end
end

# $LOAD_PATH.unshift('lib')
# require 'never_block'

require 'eventmachine'
gem('activerecord', '2.3.4'); require 'activerecord'

class ActiveRecord::ConnectionAdapters::ConnectionPool
  # Remove stale threads from the cache.
  def remove_stale_cached_threads!(cache, &block)
    # keys = Set.new(cache.keys)
    # 
    # Thread.list.each do |thread|
    #   keys.delete(thread.object_id) if thread.alive?
    # end
    # keys.each do |key|
    #   next unless cache.has_key?(key)
    #   block.call(key, cache[key])
    #   cache.delete(key)
    # end
  end
  
end

$LOAD_PATH << 'lib'
require 'ar_fibers'


ActiveRecord::Base.establish_connection(:adapter => 'mysql_with_fibers', :database => 'netconf_test', :username => 'root')
# ActiveRecord::Base.logger = Logger.new(STDOUT)

class SiteBox < ActiveRecord::Base
end

t = Time.new

$completed = 0

def end_task()
  EM::stop_event_loop if ($completed += 1) == 3
end



EM::run do
  @pool = ArFibers::FiberPool.new(10)
  
  @pool.spawn do
    100.times { SiteBox.find_by_sql('SELECT SLEEP(0.2)') }
    end_task
  end
  
  @pool.spawn do
    20.times { SiteBox.find_by_sql('SELECT SLEEP(0.1)') }
    10.times { SiteBox.find_by_sql('SELECT SLEEP(0.1)') }
    end_task
  end
  
  @pool.spawn do
    50.times { SiteBox.find_by_sql('SELECT SLEEP(0.4)') }
    end_task
  end
end


puts "async: #{Time.new - t}"


