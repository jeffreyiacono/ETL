# Basic ETL is, well, basic. (Who would have guessed it?)
#
# Inherit from this class and add attributes and / or override #etl to add
# more sophistication.
require 'etl/helpers'
require 'logger'

module ETL
  class Basic
    include Helpers

    attr_accessor :description
    attr_accessor :connection
    attr_reader   :logger

    def initialize attributes = {}
      attributes.keys.uniq.each do |attribute|
        self.send "#{attribute}=", attributes[attribute]
      end
      default_logger! unless attributes.keys.include?(:logger)
    end

    def config &block
      yield self if block_given?
      self
    end

    ORDERED_ETL_OPERATIONS = [
     :ensure_destination,
     :before_etl,
     :etl,
     :after_etl
    ]

    # A little metaprogramming to consolidate the generation of our
    # sql generating / # querying methods.
    #
    # This will produce methods of the form:
    #
    #   def [name] *args, &block
    #     if block_given?
    #       @[name] = block
    #     else
    #       @[name].call self, *args if @[name]
    #     end
    #   end
    #
    # for any given variable included in the method name's array
    ORDERED_ETL_OPERATIONS.each do |method|
      define_method method do |*args, &block|
        if block
          instance_variable_set("@#{method}", block)
        else
          instance_variable_get("@#{method}").
            call(self, *args) if instance_variable_get("@#{method}")
        end
      end
    end

    def run options = {}
      (ORDERED_ETL_OPERATIONS - [*options[:except]]).each do |method|
        send method
      end
    end

    def logger= logger
      @logger = logger
    end

    def logger?
      !!@logger
    end

    def query sql
      time_and_log(sql: sql) do
        connection.query sql
      end
    end

    def info data = {}
      logger.info data.merge(emitter: self) if @logger
    end

    def debug data = {}
      logger.debug data.merge(emitter: self) if @logger
    end

    def default_logger!
      @logger = default_logger
    end

  protected

    def default_logger
      ::Logger.new(STDOUT).tap do |logger|
        logger.formatter = proc do |severity, datetime, progname, msg|
          lead  = "[#{datetime}] #{severity} #{msg[:event_type]}"
          desc  = "\"#{msg[:emitter].description || 'no description given'}\""
          desc += " (object #{msg[:emitter].object_id})"

          case msg[:event_type]
          when :query_start
            "#{lead} for #{desc}\n#{msg[:sql]}\n"
          when :query_complete
            "#{lead} for #{desc} runtime: #{msg[:runtime]}s\n"
          else
            "#{msg}"
          end
        end
      end
    end

    def time_and_log data = {}, &block
      start_runtime = Time.now
      debug data.merge(event_type: :query_start)
      retval = yield
      info data.merge(event_type: :query_complete,
                      runtime: Time.now - start_runtime)
      retval
    end
  end
end
