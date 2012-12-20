require 'mysql2'
require 'active_support/core_ext'
require 'spec_helper'
require './lib/etl/incrementer'

def reset_test_env connection, &block
  connection.query %[DROP DATABASE IF EXISTS etl_test]
  connection.query %[CREATE DATABASE etl_test]
  connection.query %[USE etl_test]

  if block_given?
    yield connection
  else
    connection.query %[
      CREATE TABLE etl_source (
        id INT NOT NULL,
        name VARCHAR(10),
        amount INT(11) DEFAULT 0,
        PRIMARY KEY (id)
      )
    ]

    connection.query %[
      INSERT INTO etl_test.etl_source (id, name, amount)
      VALUES
        (1, 'Jeff', 100),
        (2, 'Ryan', 50),
        (3, 'Jack', 75),
        (4, 'Jeff', 10),
        (5, 'Jack', 45),
        (6, 'Nick', -90),
        (7, 'Nick', 90)
    ]
  end
end

describe ETL::Incrementer do
  it_behaves_like "basic etl", described_class

  describe '#run over full table' do
    let(:connection) { Mysql2::Client.new host: 'localhost', username: 'root', database: 'etl_test' }
    let(:etl)        { described_class.new connection: connection }

    before { reset_test_env connection }
    after  { connection.close }

    it "executes the specified sql in the appropriate order and ETLs properly" do
      etl.ensure_destination do |e|
        e.query %[
          CREATE TABLE etl_destination (
          id INT NOT NULL,
          name VARCHAR(10),
          amount INT(11) DEFAULT 0,
          PRIMARY KEY (id))
        ]
      end

      etl.before_etl do |e|
        e.query "DELETE FROM etl_source WHERE amount < 0"
      end

      etl.start do |e|
        e.query(
          "SELECT COALESCE(MAX(id), 0) AS the_start FROM etl_destination"
        ).to_a.first['the_start']
      end

      etl.step do
        1
      end

      etl.stop do |e|
        e.query(
          "SELECT MAX(id) AS the_stop FROM etl_source"
        ).to_a.first['the_stop']
      end

      etl.etl do |e, lbound, ubound|
        e.query %[
          REPLACE INTO etl_destination
            SELECT id, name, amount FROM etl_source s
            WHERE
                  s.id >= #{lbound}
              AND s.id <  #{ubound}
          ]
      end

      etl.after_etl do |e|
        e.query %[
          UPDATE etl_destination
          SET name = CONCAT("SUPER ", name)
          WHERE id <= 1
        ]
      end

      etl.run

      connection
        .query("SELECT * FROM etl_destination ORDER BY id ASC")
        .to_a
        .should == [
          {'id' => 1, 'name' => 'SUPER Jeff', 'amount' => 100},
          {'id' => 2, 'name' => 'Ryan',       'amount' => 50},
          {'id' => 3, 'name' => 'Jack',       'amount' => 75},
          {'id' => 4, 'name' => 'Jeff',       'amount' => 10},
          {'id' => 5, 'name' => 'Jack',       'amount' => 45},
          {'id' => 7, 'name' => 'Nick',       'amount' => 90}]
    end
  end

  describe '#run over part of table' do
    let(:connection) { Mysql2::Client.new host: 'localhost', username: 'root', database: 'etl_test' }
    let(:etl)        { described_class.new connection: connection }

    before { reset_test_env connection }
    after  { connection.close }

    it "executes the specified sql in the appropriate order and ETLs properly" do
      etl.ensure_destination do |e|
        e.query %[
          CREATE TABLE etl_destination (
            id INT NOT NULL,
            name VARCHAR(10),
            amount INT(11) DEFAULT 0,
            PRIMARY KEY (id))
        ]
      end

      etl.before_etl do |e|
        e.query "DELETE FROM etl_source WHERE amount < 0"
      end

      etl.start do
        4
      end

      etl.step do
        1
      end

      etl.stop do |e|
        e.query(
          "SELECT MAX(id) AS the_stop FROM etl_source"
        ).to_a.first['the_stop']
      end

      etl.etl do |e, lbound, ubound|
        e.query %[
          REPLACE INTO etl_destination
            SELECT id, name, amount FROM etl_source s
            WHERE
                  s.id >= #{lbound}
              AND s.id <  #{ubound}
          ]
      end

      etl.run

      connection
        .query("SELECT * FROM etl_destination ORDER BY id ASC")
        .to_a.should == [
          {'id' => 4, 'name' => 'Jeff', 'amount' => 10},
          {'id' => 5, 'name' => 'Jack', 'amount' => 45},
          {'id' => 7, 'name' => 'Nick', 'amount' => 90}]
    end
  end

  describe "#run over gappy data" do
    let(:connection) { Mysql2::Client.new host: 'localhost', username: 'root', database: 'etl_test' }
    let(:etl)        { described_class.new connection: connection }

    before do
      reset_test_env(connection) do |connection|
        connection.query %[
          CREATE TABLE etl_source (
            id INT NOT NULL,
            name VARCHAR(10),
            amount INT(11) DEFAULT 0,
            PRIMARY KEY (id))
        ]

        connection.query %[
          INSERT INTO etl_source (id, name, amount)
          VALUES
            (1, 'Jeff', 100),
            (2, 'Ryan', 50),
            (13, 'Jack', 75),
            (14, 'Jeff', 10),
            (15, 'Jack', 45),
            (16, 'Nick', -90),
            (17, 'Nick', 90)
        ]
      end
    end

    after { connection.close }

    it "executes the specified sql in the appropriate order without getting stuck" do
      etl.ensure_destination do |e|
        e.query %[
         CREATE TABLE etl_destination (
          id INT NOT NULL,
          name VARCHAR(10),
          amount INT(11) DEFAULT 0,
          PRIMARY KEY (id))
        ]
      end

      etl.before_etl do |e|
        e.query "DELETE FROM etl_source WHERE amount < 0"
      end

      etl.start do |e|
        1
      end

      etl.step do
        1
      end

      etl.stop do |e|
        e.query(
          "SELECT MAX(id) AS the_stop FROM etl_source"
        ).to_a.first['the_stop']
      end

      etl.etl do |e, lbound, ubound|
        e.query %[
          REPLACE INTO etl_destination
            SELECT
                id
              , name
              , amount
            FROM etl_source s
            WHERE
                  s.id >= #{lbound}
              AND s.id <  #{ubound}
        ]
      end

      etl.run

      connection
        .query("SELECT * FROM etl_destination ORDER BY id ASC")
        .to_a
        .should == [
          {'id' => 1,  'name' => 'Jeff', 'amount' => 100},
          {'id' => 2,  'name' => 'Ryan', 'amount' => 50},
          {'id' => 13, 'name' => 'Jack', 'amount' => 75},
          {'id' => 14, 'name' => 'Jeff', 'amount' => 10},
          {'id' => 15, 'name' => 'Jack', 'amount' => 45},
          {'id' => 17, 'name' => 'Nick', 'amount' => 90}]
    end
  end

  describe "#run over date data" do
    let(:connection) { Mysql2::Client.new host: 'localhost', username: 'root', database: 'etl_test' }
    let(:etl)        { described_class.new connection: connection }

    before do
      reset_test_env(connection) do |connection|
        connection.query %[
          CREATE TABLE etl_source (
            the_date DATE NOT NULL,
            name VARCHAR(10),
            amount INT(11) DEFAULT 0
          )
        ]

        connection.query %[
          INSERT INTO etl_source (the_date, name, amount)
          VALUES
            ('2012-01-01', 'Jeff', 100),
            ('2012-01-01', 'Ryan', 50),
            ('2012-01-01', 'Jack', 75),
            ('2012-01-01', 'Jeff', 10),
            ('2012-01-02', 'Jack', 45),
            ('2012-01-02', 'Nick', -90),
            ('2012-01-02', 'Nick', 90)
        ]
      end
    end

    after { connection.close }

    it "executes the specified sql in the appropriate order and ETLs properly" do
      etl.ensure_destination do |e|
        e.query %[
          CREATE TABLE etl_destination (
          the_date DATE NOT NULL,
          name VARCHAR(10),
          total_amount INT(11) DEFAULT 0,
          PRIMARY KEY (the_date, name))
        ]
      end

      etl.before_etl do |e|
        e.query "DELETE FROM etl_source WHERE amount < 0"
      end

      etl.start do |e|
        e.query(%[
          SELECT COALESCE(MAX(the_date), DATE('2012-01-01')) AS the_start
          FROM etl_destination
        ]).to_a.first['the_start']
      end

      etl.step do
        1.day
      end

      etl.stop do |e|
        e.query(
          "SELECT MAX(the_date) AS the_stop FROM etl_source"
        ).to_a.first['the_stop']
      end

      etl.etl do |e, lbound, ubound|
        e.query %[
          REPLACE INTO etl_destination
            SELECT
                the_date
              , name
              , SUM(amount) AS total_amount
            FROM etl_source s
            WHERE
                  s.the_date >= '#{lbound}'
              AND s.the_date <  '#{ubound}'
            GROUP BY
                the_date
              , name
        ]
      end

      etl.run

      connection
        .query(%[
          SELECT
              the_date
            , name
            , total_amount
          FROM
            etl_destination
          ORDER BY
              the_date ASC
            , name ASC
        ]).to_a
          .should == [
            {'the_date' => Date.parse('2012-01-01'), 'name' => 'Jack', 'total_amount' => 75},
            {'the_date' => Date.parse('2012-01-01'), 'name' => 'Jeff', 'total_amount' => 110},
            {'the_date' => Date.parse('2012-01-01'), 'name' => 'Ryan', 'total_amount' => 50},
            {'the_date' => Date.parse('2012-01-02'), 'name' => 'Jack', 'total_amount' => 45},
            {'the_date' => Date.parse('2012-01-02'), 'name' => 'Nick', 'total_amount' => 90}]
    end
  end

  describe "#run over datetime data" do
    let(:connection) { Mysql2::Client.new host: 'localhost', username: 'root', database: 'etl_test' }
    let(:etl)        { described_class.new connection: connection }

    before do
      reset_test_env(connection) do |connection|
        connection.query %[
          CREATE TABLE etl_source (
            the_datetime DATETIME NOT NULL,
            name VARCHAR(10),
            amount INT(11) DEFAULT 0)
        ]

        connection.query %[
          INSERT INTO etl_source (the_datetime, name, amount)
          VALUES
            ('2011-12-31 23:59:59', 'Jeff', 100),
            ('2012-01-01 00:01:00', 'Ryan', 50),
            ('2012-01-01 00:01:01', 'Jack', 75),
            ('2012-01-01 00:01:02', 'Jeff', 10),
            ('2012-01-02 00:02:00', 'Jack', 45),
            ('2012-01-02 00:02:01', 'Nick', -90),
            ('2012-01-02 00:02:02', 'Nick', 90)
        ]
      end
    end

    after { connection.close }

    it "executes the specified sql in the appropriate order and ETLs properly" do
      etl.ensure_destination do |e|
        e.query %[
          CREATE TABLE etl_destination (
          the_datetime DATETIME NOT NULL,
          name VARCHAR(10),
          amount INT(11) DEFAULT 0,
          PRIMARY KEY (the_datetime, name))
        ]
      end

      etl.before_etl do |e|
        e.query "DELETE FROM etl_source WHERE amount < 0"
      end

      etl.start do |e|
        e.query(%[
          SELECT CAST(COALESCE(MAX(the_datetime), '2012-01-01 00:00:00') AS DATETIME) AS the_start
          FROM etl_destination
        ]).to_a.first['the_start']
      end

      etl.step do
        1.minute
      end

      etl.stop do |e|
        e.query(
          "SELECT MAX(the_datetime) AS the_stop FROM etl_source"
        ).to_a.first['the_stop']
      end

      etl.etl do |e, lbound, ubound|
        e.query %[
          REPLACE INTO etl_destination
            SELECT
                the_datetime
              , name
              , amount
            FROM etl_source s
            WHERE
                  s.the_datetime >= '#{lbound}'
              AND s.the_datetime <  '#{ubound}'
        ]
      end

      etl.run

      connection
        .query(%[
          SELECT
              the_datetime
            , name
            , amount
          FROM
            etl_destination
          ORDER BY
              the_datetime ASC
            , name ASC
        ]).to_a
          .should == [
            {'the_datetime' => Time.parse('2012-01-01 00:01:00'), 'name' => 'Ryan', 'amount' => 50},
            {'the_datetime' => Time.parse('2012-01-01 00:01:01'), 'name' => 'Jack', 'amount' => 75},
            {'the_datetime' => Time.parse('2012-01-01 00:01:02'), 'name' => 'Jeff', 'amount' => 10},
            {'the_datetime' => Time.parse('2012-01-02 00:02:00'), 'name' => 'Jack', 'amount' => 45},
            {'the_datetime' => Time.parse('2012-01-02 00:02:02'), 'name' => 'Nick', 'amount' => 90}]
    end
  end
end
