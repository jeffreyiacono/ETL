require 'date'
require 'time'
require 'etl/basic'

# Incrementer ETL starts at the specified start and increments over the range
# by the specified step up until the stop.
#
# Note that we want to memoize the start, step, and stop attributes because
# we could get into a situation where a source table is growing faster than a
# destination table can be built, resulting in an infinite loop, which would
# make us sad.
module ETL
  class Incrementer < ETL::Basic
    # A little metaprogramming to consolidate the generation of our
    # sql generating / querying methods.
    #
    # This will produce methods of the form:
    #
    #   def [method] *args, &block
    #     if block
    #       @_[method]_block = block
    #     else
    #       # cache block's result
    #       if defined? @[method]
    #         @[method]
    #       else
    #         @[method] = @_[method]_block.call(self, *args)
    #       end
    #     end
    #   end
    #
    # for any given variable included in the method name's array
    [:start,
     :step,
     :stop
    ].each do |method|
      define_method method do |*args, &block|
        if block
          instance_variable_set("@_#{method}_block", block)
        else
          if instance_variable_defined?("@#{method}")
            instance_variable_get("@#{method}")
          else
            instance_variable_set("@#{method}",
                                  instance_variable_get("@_#{method}_block")
                                    .call(self, *args))
          end
        end
      end
    end

    def etl *args, &block
      if block_given?
        super *args, block
      else
        current = start
        super cast(current), cast(current += step) while stop >= current
      end
    end

  private

    # NOTE: If you needed to handle more type data type casting you can add a
    # case statement. If you need to be able to handle entirely different sets
    # of casting depending on database engine, you can modify #cast to take a
    # "type" arg and then determine which caster to route the arg through
    def cast arg
      case arg
      when Date then arg.strftime("%Y-%m-%d")
      when Time then arg.strftime("%Y-%m-%d %H:%M:%S")
      else
        arg
      end
    end
  end
end
