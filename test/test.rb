require 'rubygems'

# test should not create any thread
class Thread
  def self.new(*args)
    raise
  end
end

require 'eventmachine'
gem('activerecord', '2.3.4'); require 'activerecord'

$LOAD_PATH << 'lib'
require 'ar_fibers'


ActiveRecord::Base.establish_connection(:adapter => 'mysql_with_fibers', :database => 'mysql', :username => 'root', :pool => 10)
ActiveRecord::Base.logger = Logger.new(STDOUT)

class SiteBox < ActiveRecord::Base
end

t = Time.new

$completed = 0

def end_task()
  if ($completed += 1) == 3
    EM::stop_event_loop
  end
end



EM::run do
  @pool = ArFibers::FiberPool.new(10)
  
  @pool.spawn do
    10.times { SiteBox.find_by_sql('SELECT SLEEP(1)') }
    end_task
  end
  
  @pool.spawn do
    10.times { SiteBox.find_by_sql('SELECT SLEEP(1)') }
    5.times { SiteBox.find_by_sql('SELECT SLEEP(1)') }
    end_task
  end
  
  @pool.spawn do
    20.times { SiteBox.find_by_sql('SELECT SLEEP(0.5)') }
    end_task
  end
end


puts "async: #{Time.new - t} =~ 15s"


