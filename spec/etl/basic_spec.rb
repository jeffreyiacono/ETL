require 'mysql2'
require 'spec_helper'
require './lib/etl/basic'

describe ETL::Basic do
  it_behaves_like "basic etl", described_class

  describe 'max_for' do
    let(:connection) { Mysql2::Client.new host: 'localhost', username: 'root', database: 'etl_test' }
    let(:etl) { ETL::Basic.new connection: connection }

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
          the_time_at DATETIME DEFAULT NULL,
          PRIMARY KEY (id)
        )
      ]

      client.query %[
        INSERT INTO etl_source (name, amount, the_date, the_time_at)
        VALUES
          ('Jeff', 100, '2012-01-02', '2012-01-02 00:00:01'),
          ('Ryan', 50, '2012-01-01', '2012-01-01 00:00:00'),
          ('Jack', 75, '2012-01-01', '2012-01-01 00:00:00'),
          ('Jeff', 10, '2012-01-01', '2012-01-01 00:00:00'),
          ('Jack', 45, '2012-01-01', '2012-01-01 00:00:00'),
          ('Nick', -90, '2012-01-01', '2012-01-01 00:00:00'),
          ('Nick', 90, '2012-01-01', '2012-01-01 00:00:00')
      ]

      client.close
    end

    after { connection.close }

    it "finds the max for dates" do
      etl.max_for(database: :etl_test,
                  table:    :etl_source,
                  column:   :the_date).should == Date.parse('2012-01-02')
    end

    it "finds the max for dates when a default floor is provided" do
      etl.max_for(database:      :etl_test,
                  table:         :etl_source,
                  column:        :the_date,
                  default_floor: '2012-01-01').should == Date.parse('2012-01-02')
    end

    it "finds the max for dates" do
      etl.max_for(database: :etl_test,
                  table:    :etl_source,
                  column:   :the_date).should == Date.parse('2012-01-02')
    end

    it "finds the max for datetimes" do
      etl.max_for(database: :etl_test,
                  table:    :etl_source,
                  column:   :the_time_at).should == Date.parse('2012-01-02')
    end

    it "finds the max for datetimes when a default floor is provided" do
      etl.max_for(database:      :etl_test,
                  table:         :etl_source,
                  column:        :the_time_at,
                  default_floor: '2012-01-01 00:00:00').should == Date.parse('2012-01-02')
    end

    it "raises an error if a non-standard column is supplied with no default floor" do
      expect {
        etl.max_for(database: :etl_test, table: :etl_source, column: :amount)
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
    let(:etl)        { ETL::Basic.new connection: connection }

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
end
