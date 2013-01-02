# Basic ETL is, well, basic. (Who would have guessed it?)
#
# Inherit from this class and add attributes and / or override #etl to add
# more sophistication.

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

    def run options = {}
      (ORDERED_ETL_OPERATIONS - [*options[:except]]).each do |method|
        send method
      end
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
      time_and_log(event_type: :query, sql: sql) do
        connection.query sql
      end
    end

    def log data = {}
      @logger.log data.merge(emitter: self) if @logger
    end

    def warn data = {}
      @logger.warn data.merge(emitter: self) if @logger
    end

  protected

    def time_and_log data = {}, &block
      start_runtime = Time.now
      retval = yield
      log data.merge(runtime: Time.now - start_runtime)
      retval
    end
  end
end
