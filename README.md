# ETL

Extract, transform, and load data with ruby!

## Installation

Add this line to your application's Gemfile:

    gem 'etl'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install etl

## ETL Dependencies

Both ETLs depend on having a database connection that __must__ respond to
`#query`. [mysql2](https://github.com/brianmario/mysql2) is a good option. You
can also proxy another library using Ruby's `SimpleDelegator` and add a `#query`
method if need be.

You can optionally supply a logger that __must__ respond to `#warn` and `#log`.
Implementation of your logger of choice is left up to the developer. View the
[logger details](#logger-details) to see what is logged and when.

## ETL API

The ETL framework has two basic types: `ETL::Basic` and `ETL::Iterator`.

### ETL::Basic

The Basic ETL, as the name suggests, is pretty basic and should be used when you
want to run sequential SQL statements that do not require iteration.

A Basic ETL has the following framework:

```ruby
etl = ETL::Basic.new(description: "a description of what this ETL does",
                     logger:      logger,
                     connection:  connection)
```
which can then be configured:

```ruby
etl.config do |e|
  e.ensure_destination do |e|
    # For most ETLs you may want to ensure that the destination exists, so the
    # #ensure_destination block is ideally suited to fulfill this requirement.
    #
    # e.query <<-EOS
    #   YOUR BEFORE ETL SQL / CODE GOES HERE
    # EOS
    #
    # By way of example:
    #
    #   e.query <<-EOS
    #     CREATE TABLE IF NOT EXISTS some_database.some_table (
    #       user_id INT UNSIGNED NOT NULL,
    #       created_date DATE NOT NULL,
    #       total_amount INT SIGNED NOT NULL,
    #       PRIMARY KEY (user_id),
    #       KEY (user_id, created_date),
    #       KEY (created_date)
    #     )
    #   EOS
  end

  e.before_etl do |e|
    # All pre-ETL work is performed in this block.
    #
    # This can be thought of as a before-ETL hook that will fire only once. This
    # usage of this block is not very clear in the Basic ETL, but will in the
    # Iterator ETL when we introduce iteration.
    #
    # Again, the following convention is used:
    #
    # e.query <<-EOS
    #   YOUR BEFORE ETL SQL / CODE GOES HERE
    # EOS
    #
    # As an example, let's say we want to get rid of all entries that have an
    # amount less than zero before moving on to our actual etl:
    #
    #   e.query <<-EOS
    #     DELETE FROM some_database.some_source_table WHERE amount < 0
    #   EOS
  end

  e.etl do |e|
    # Here is where the magic happens! This block contains the main ETL SQL.
    # The following convention is used:
    #
    # e.query <<-EOS
    #   YOU ETL SQL / CODE GOES HERE
    # EOS
    #
    # For example:
    #
    #   e.query <<-EOS
    #     REPLACE INTO some_database.some_destination
    #     SELECT
    #         user_id
    #       , DATE(created_at) AS created_date
    #       , SUM(amount) AS total_amount
    #     FROM
    #       some_database.some_source_table sst
    #     GROUP BY
    #         sst.user_id
    #       , sst.DATE(created_at)
    #   EOS
  end

  e.after_etl do |e|
    # All post-ETL work is performed in this block.
  end
end
```

At this point it is possible to run the ETL instance via:

```ruby
etl.ensure_destination
etl.before_etl
etl.etl
etl.after_etl
```

or via the more concise:

```ruby
etl.run
```
which executes `#ensure_destination`, `#before_etl`, `#etl`, and `#after_etl` in
that order.

### ETL::Iterator

The Iterator ETL provides all the functionality of the Basic ETL but additionally provides
the ability to iterate over a data set in the `#etl` block. When dealing with very large data sets
or executing queries that, while optimized, are still slow then the Iterator ETL is recommended.

The Iterator ETL has the following framework:

```ruby
etl = ETL::Iterator.new(description: "a description of what this ETL does",
                        logger:      logger,
                        connection:  connection)
```

where `logger` and `connection` are the same as described above.

Next we can configure the ETL:

```ruby
# assuming we have the ETL instance from above
etl.config do |e|
  e.ensure_destination do |e|
    # For most ETLs you may want to ensure that the destination exists, so the
    # #ensure_destination block is ideally suited to fulfill this requirement.
    #
    # e.query <<-EOS
    #   YOUR BEFORE ETL SQL / CODE GOES HERE
    # EOS
    #
    # By way of example:
    #
    #   e.query <<-EOS
    #     CREATE TABLE IF NOT EXISTS some_database.some_table (
    #       user_id INT UNSIGNED NOT NULL,
    #       created_date DATE NOT NULL,
    #       total_amount INT SIGNED NOT NULL,
    #       PRIMARY KEY (user_id),
    #       KEY (user_id, created_date),
    #       KEY (created_date)
    #     )
    #   EOS
  end

  e.before_etl do |e|
    # All pre-ETL work is performed in this block.
    #
    # This can be thought of as a before-ETL hook that will fire only once. This
    # usage of this block is not very clear in the Basic ETL, but will in the
    # Iterator ETL when we introduce iteration.
    #
    # Again, the following convention is used:
    #
    # e.query <<-EOS
    #   YOUR BEFORE ETL SQL / CODE GOES HERE
    # EOS
    #
    # As an example, let's say we want to get rid of all entries that have an
    # amount less than zero before moving on to our actual etl:
    #
    #   e.query <<-EOS
    #     DELETE FROM some_database.some_source_table WHERE amount < 0
    #   EOS
  end

  e.start do |e|
    # This defines where the ETL should start. This can be a flat number
    # or date, or even SQL / other code can be executed to produce a starting
    # value.
    #
    # Usually, this is the last known entry for the destination table with
    # some sensible default if the destination does not yet contain data.
    #
    # As an example:
    #
    #   res = e.query <<-EOS
    #     SELECT MAX(id) AS the_max FROM some_database.some_destination_table
    #   EOS
    #   res.to_a.first['the_max']
  end

  e.step do |e|
    # The step block defines the size of the iteration block. To iterate by
    # ten records, the step block should be set to return 10.
    #
    # As an alternative example, to set the iteration to go 10,000 units
    # at a time, the following value should be provided:
    #
    #   10_000 (Note: An underscore is used in place of a comma.)
    #
    # And, when working with dates, the step block should be set to a number
    # of days. To iterate over 7 days at a time, then use:
    #
    #   7.days
  end

  e.stop do |e|
    # The stop block defines when the iteration should halt.
    # Again, this can be a flat value or code. Either way, one value *must* be
    # returned.
    #
    # As a flat value:
    #
    #   1_000_000
    #
    # Or a date value:
    #
    #   Time.now.to_date
    #
    # Or as a code example:
    #
    #   res = e.query <<-EOS
    #     SELECT MAX(id) AS the_max FROM some_database.some_source_table
    #   EOS
    #   res.to_a.first['the_max']
  end

  e.etl do |e, lbound, ubound|
    # The etl block is the main part of the framework. Note: there are
    # two extra args with the iterator - "lbound" and "ubound"
    #
    # "lbound" is the lower bound of the current iteration. When iterating
    # from 0 to 10 and stepping by 2, the lbound would equal 2 on the
    # second iteration.
    #
    # "ubound" is the upper bound of the current iteration. In continuing with the
    # example above, when iterating from 0 to 10 and stepping by 2, the ubound would
    # equal 4 on the second iteration.
    #
    # These args can be used to "window" SQL queries.
    #
    # As a first example, to iterate over a set of ids:
    #
    #   sql = <<-EOS
    #     REPLACE INTO some_database.some_destination
    #     SELECT
    #         user_id
    #       , SUM(amount) AS total_amount
    #     FROM
    #       some_database.some_source_table sst
    #     WHERE
    #       sst.user_id > #{lbound} AND sst.user_id <= #{ubound}
    #     GROUP BY
    #       sst.user_id
    #   EOS
    #   e.query(sql)
    #
    # To "window" a SQL query using dates:
    #
    #   sql = <<-EOS
    #     REPLACE INTO some_database.some_destination
    #     SELECT
    #         DATE(created_at)
    #       , SUM(amount) AS total_amount
    #     FROM
    #       some_database.some_source_table sst
    #     WHERE
    #       -- Note the usage of quotes surrounding the lbound and ubound vars.
    #       -- This is is required when dealing with dates / datetimes
    #       sst.created_at >= '#{lbound}' AND sst.created_at < '#{ubound}'
    #     GROUP BY
    #       sst.user_id
    #   EOS
    #   e.query(sql)
    #
    # Note that there is no sql sanitization here so there is *potential* for SQL
    # injection. That being said you'll likely be using this gem in an internal
    # tool so hopefully your co-workers are not looking to sabotage your ETL
    # pipeline. Just be aware of this and handle it as you see fit.
  end

  e.after_etl do |e|
    # All post-ETL work is performed in this block.
  end
end
```

At this point it is possible to run the ETL instance via:

```ruby
etl.ensure_destination
etl.before_etl
etl.etl
etl.after_etl
```

or via the more concise:

```ruby
etl.run
```
which executes `#ensure_destination`, `#before_etl`, `#etl`, and `#after_etl` in
that order.

Note that `#etl` executes `#start` and `#stop` once and memoizes the result for
each. It then begins to iterate from what `#start` evaluated to up until what `#stop`
evaluated to by what `#step` evaluates to.

## Logger Details

A logger must support two methods: `#log` and `#warn`. The ETL framework will
enforce this interface and raise an exception if it is not met.

Both methods should accept a single hash argument. The argument will contain:

- `:emitter` => a reference to the ETL instance's `self`
- `:event_type` => a symbol that includes the type of event being logged. You
  can use this value to derive which other data you'll have available

When `:event_type` is equal to `:query`, you'll have the follow available in the
hash argument:

- `:sql` => the sql that was run
- `:runtime` => how long the query took to execute

The ETL gem enforces that the passed logger implement `warn` but this is for
future use and is currently not used to report execeptions or other calamitous
events.

Following from this you could implement a simple logger as:

```ruby
class PutsLogger
  def log data
    case (event_type = data.delete(:event_type))
    when :query
      output =  "#{data[:emitter].description} executed #{data[:sql]}, "
      output += "which completed at #{Time.now} and took #{data[:runtime]} "
      output += "seconds to complete."
    else
      output = "no special logging for #{event_type} event_type yet"
    end
    puts output
  end

  def warn data
    case (event_type = data.delete(:event_type))
    when :query
      output =  "[WARN] this is currently not called by the ETL gem, "
      output += "but is here for future use and for the sake of completeness."
    else
      output = "no special warning for #{event_type} event_type yet"
    end
    puts output
  end
end
```

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Added some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request

## License

Copyright (c) 2012 Jeff Iacono & Square Inc.

MIT License

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
