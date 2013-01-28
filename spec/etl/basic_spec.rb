require 'mysql2'
require 'etl/basic'

describe ETL::Basic do
  let(:logger) { nil }

  describe "logger=" do
    let(:etl) { described_class.new connection: stub }

    it 'assigns when the param responds to #log and #warn' do
      logger = stub
      etl.logger = logger
      etl.logger.should == logger
    end
  end

  describe 'max_for' do
    let(:connection) { Mysql2::Client.new host: 'localhost', username: 'root', database: 'etl_test' }
    let(:etl)        { ETL::Basic.new connection: connection, logger: logger }

    before do
      client = Mysql2::Client.new host: 'localhost', username: 'root'
      client.query %[DROP DATABASE IF EXISTS etl_test]
      client.query %[CREATE DATABASE etl_test]
      client.query %[USE etl_test]
      client.query %[
        CREATE TABLE IF NOT EXISTS etl_source (
          id INT(11) NOT NULL AUTO_INCREMENT,
          name VARCHAR(10),
          amount INT(11) DEFAULT 0,
          the_date DATE DEFAULT NULL,
          the_null_date DATE DEFAULT NULL,
          the_time_at DATETIME DEFAULT NULL,
          the_null_time_at DATETIME DEFAULT NULL,
          PRIMARY KEY (id)
        )
      ]

      client.query %[
        INSERT INTO etl_source (
            name
          , amount
          , the_date
          , the_null_date
          , the_time_at
          , the_null_time_at
        ) VALUES
          ('Jeff', 100, '2012-01-02', NULL, '2012-01-02 00:00:01', NULL),
          ('Ryan',  50, '2012-01-01', NULL, '2012-01-01 00:00:00', NULL),
          ('Jack',  75, '2012-01-01', NULL, '2012-01-01 00:00:00', NULL),
          ('Jeff',  10, '2012-01-01', NULL, '2012-01-01 00:00:00', NULL),
          ('Jack',  45, '2012-01-01', NULL, '2012-01-01 00:00:00', NULL),
          ('Nick', -90, '2012-01-01', NULL, '2012-01-01 00:00:00', NULL),
          ('Nick',  90, '2012-01-01', NULL, '2012-01-01 00:00:00', NULL)
      ]

      client.close
    end

    after { connection.close }

    it "finds the max for dates" do
      etl.max_for(database: :etl_test,
                  table:    :etl_source,
                  column:   :the_date).should == Date.parse('2012-01-02')
    end

    it "defaults to the beginning of time date when a max date cannot be found" do
      etl.max_for(database:      :etl_test,
                  table:         :etl_source,
                  column:        :the_null_date).should == Date.parse('1970-01-01')
    end

    it "defaults to the specified default floor when a max date cannot be found" do
      etl.max_for(database:      :etl_test,
                  table:         :etl_source,
                  column:        :the_null_date,
                  default_floor: '2011-01-01').should == Date.parse('2011-01-01')
    end

    it "finds the max for datetimes" do
      etl.max_for(database: :etl_test,
                  table:    :etl_source,
                  column:   :the_time_at).should == Date.parse('2012-01-02')
    end

    it "defaults to the beginning of time when a max datetime cannot be found" do
      etl.max_for(database: :etl_test,
                  table:    :etl_source,
                  column:   :the_null_time_at).should == Date.parse('1970-01-01 00:00:00')
    end

    it "defaults to the specified default floor when a max datetime cannot be found" do
      etl.max_for(database:      :etl_test,
                  table:         :etl_source,
                  column:        :the_null_time_at,
                  default_floor: '2011-01-01 00:00:00').should == Date.parse('2011-01-01 00:00:00')
    end

    it "raises an error if a non-standard column is supplied with no default floor" do
      expect {
        etl.max_for database: :etl_test,
                    table:    :etl_source,
                    column:   :amount
      }.to raise_exception
    end

    it "finds the max for a non-standard column, using the default floor" do
      etl.max_for(database:      :etl_test,
                  table:         :etl_source,
                  column:        :amount,
                  default_floor: 0).should == 100
    end
  end

  describe '#run' do
    let(:connection) { Mysql2::Client.new host: 'localhost', username: 'root', database: 'etl_test' }
    let(:etl)        { ETL::Basic.new connection: connection, logger: logger }

    before do
      client = Mysql2::Client.new host: 'localhost', username: 'root'
      client.query %[DROP DATABASE IF EXISTS etl_test]
      client.query %[CREATE DATABASE etl_test]
      client.query %[USE etl_test]
      client.query %[
        CREATE TABLE IF NOT EXISTS etl_source (
          id INT(11) NOT NULL AUTO_INCREMENT,
          name VARCHAR(10),
          amount INT(11) DEFAULT 0,
          PRIMARY KEY (id)
        )
      ]

      client.query %[
        INSERT INTO etl_source (name, amount)
        VALUES
          ('Jeff', 100),
          ('Ryan', 50),
          ('Jack', 75),
          ('Jeff', 10),
          ('Jack', 45),
          ('Nick', -90),
          ('Nick', 90)
      ]

      client.close
    end

    it "executes the specified sql in the appropriate order" do
      etl.ensure_destination do |e|
        e.query %[
          CREATE TABLE IF NOT EXISTS etl_destination (
            name VARCHAR(10),
            total_amount INT(11) DEFAULT 0,
            PRIMARY KEY (name)
          )
        ]
      end

      etl.before_etl do |e|
        e.query "DELETE FROM etl_source WHERE amount < 0"
      end

      etl.etl do |e|
        e.query %[
          REPLACE INTO etl_destination
          SELECT name, SUM(amount) FROM etl_source
          GROUP BY name
        ]
      end

      etl.after_etl do |e|
        e.query %[
          UPDATE etl_destination
          SET name = CONCAT("SUPER ", name)
          WHERE total_amount > 115
        ]
      end

      etl.run

      connection
        .query("SELECT * FROM etl_destination ORDER BY total_amount DESC")
        .to_a
        .should == [
          {'name' => 'SUPER Jack', 'total_amount' => 120},
          {'name' => 'Jeff',       'total_amount' => 110},
          {'name' => 'Nick',       'total_amount' => 90},
          {'name' => 'Ryan',       'total_amount' => 50}
      ]
    end
  end

  describe '#run operations specified for exclusion' do
    let(:connection) { stub }
    let(:etl)        { ETL::Basic.new connection: connection, logger: logger }

    it "does not call the specified method" do
      etl.ensure_destination {}
      etl.should_not_receive(:ensure_destination)
      etl.run except: :ensure_destination
    end
  end
end
