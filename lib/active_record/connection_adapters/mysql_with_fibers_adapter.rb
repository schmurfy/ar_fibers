# Require original mysql adapter as we'll just extend it
require 'active_record/connection_adapters/mysql_adapter'

require File.expand_path('../../../fibered_db_connection', __FILE__)
require File.expand_path('../../../fibered_mysql_connection', __FILE__)
require File.expand_path('../../../connection_pool_with_fibers', __FILE__)

class Fiber
  
  #Attribute Reference--Returns the value of a fiber-local variable, using
  #either a symbol or a string name. If the specified variable does not exist,
  #returns nil.
  def [](key)
    local_fiber_variables[key]
  end
  
  #Attribute Assignment--Sets or creates the value of a fiber-local variable,
  #using either a symbol or a string. See also Fiber#[].
  def []=(key,value)
    local_fiber_variables[key] = value
  end
  
  private
  
  def local_fiber_variables
    @local_fiber_variables ||= {}
  end
end

module ActiveRecord
  module ConnectionAdapters
    class MysqlWithFibersAdapter < ActiveRecord::ConnectionAdapters::MysqlAdapter

      # Returns 'NeverBlockMySQL' as adapter name for identification purposes
      def adapter_name
        'MysqlWithFibers'
      end

      def configure_connection
        encoding = @config[:encoding]
        if encoding
          Fiber.current[:blocking] = true
          execute("SET NAMES '#{encoding}'")
        end
    
        # By default, MySQL 'where id is null' selects the last inserted id.
        # Turn this off. http://dev.rubyonrails.org/ticket/6778
        Fiber.current[:blocking] = true
        execute("SET SQL_AUTO_IS_NULL=0")
      end
    end
  end
end


class ActiveRecord::Base
  # Establishes a connection to the database that's used by all Active Record objects.
  def self.mysql_with_fibers_connection(config) # :nodoc:
      config = config.symbolize_keys
      host     = config[:host]
      port     = config[:port]
      socket   = config[:socket]
      username = config[:username] ? config[:username].to_s : 'root'
      password = config[:password].to_s

      if config.has_key?(:database)
        database = config[:database]
      else
        raise ArgumentError, "No database specified. Missing argument: database."
      end

      # Require the MySQL driver and define Mysql::Result.all_hashes
      unless defined? Mysql
        begin
          require_library_or_gem('mysqlplus')
        rescue LoadError
          $stderr.puts 'mysqlplus is required'
          raise
        end
      end
      MysqlCompat.define_all_hashes_method!
      
      mysql = ArFibers::FiberedMysqlConnection.init
      mysql.ssl_set(config[:sslkey], config[:sslcert], config[:sslca], config[:sslcapath], config[:sslcipher]) if config[:sslca] || config[:sslkey]

      ::ActiveRecord::ConnectionAdapters::MysqlWithFibersAdapter.new(mysql, logger, [host, username, password, database, port, socket], config)
    end
end
