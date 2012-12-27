# Basic ETL is, well, basic. (Who would have guessed it?)
#
# Parameters include a database connection, logger, description.
#
# Many ETLs can subclass this and add attributes and / or override #etl to be
# more sophisticated.

require 'etl/helpers'

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
          instance_variable_get("@#{method}")
            .call(self, *args) if instance_variable_get("@#{method}")
        end
      end
    end

    def run
      ORDERED_ETL_OPERATIONS.each { |method| send method }
    end

    def logger= logger
      [:log,
       :warn
      ].each do |required_method|
        unless logger.respond_to? required_method
          raise ArgumentError, <<-EOS
            logger must implement ##{required_method}
          EOS
        end
      end

      @logger = logger
    end

    def query sql
      time_and_log(query: sql) { connection.query sql }
    end

    def log information = {}
      @logger.log information.merge(emitter: self) if @logger
    end

    def warn information = {}
      @logger.warn information.merge(emitter: self) if @logger
    end

  protected

    # take a hash of information and a block and starts a timer and then yields
    # to the given block - it then calls #log with the information hash and
    # merges in the runtime and returns the yielded block's return value
    def time_and_log information = {}, &block
      start_runtime = Time.now
      retval = yield
      log information.merge(runtime: Time.now - start_runtime)
      retval
    end
  end
end
