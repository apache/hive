-- This test verifies that if the functions and tables unregistered when the database is dropped
-- and other databases are not affected

CREATE DATABASE TEST_database;

USE TEST_database;

CREATE TABLE test_table (key STRING, value STRING);

CREATE FUNCTION test_func as 'org.apache.hadoop.hive.ql.udf.UDFAscii';

USE default;

CREATE TABLE test_table (key STRING, value STRING);

CREATE FUNCTION test_func as 'org.apache.hadoop.hive.ql.udf.UDFAscii';

DROP DATABASE TEST_database CASCADE;

describe test_table;

describe function test_func;

describe function TEST_database.test_func;

describe TEST_database.test_table;
