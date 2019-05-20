CREATE DATABASE sometableshavehook;
USE sometableshavehook;

CREATE TABLE NOHOOK0 (name string, number int);
CREATE TABLE NOHOOK1 (name string, number int);
CREATE TABLE NOHOOK2 (name string, number int);
CREATE TABLE NOHOOK3 (name string, number int);
CREATE TABLE NOHOOK4 (name string, number int);

CREATE TABLE HBASEHOOK0 (key int, val binary)
    STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
    WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key,cf:val#b"
);
CREATE TABLE HBASEHOOK1 (key int, val binary)
    STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
    WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key,cf:val#b"
);
CREATE TABLE HBASEHOOK2 (key int, val binary)
    STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
    WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key,cf:val#b"
);

set metastore.batch.retrieve.max=5;
DROP DATABASE sometableshavehook CASCADE;
SHOW DATABASES;

CREATE DATABASE sometableshavehook;
USE sometableshavehook;

CREATE TABLE NOHOOK0 (name string, number int);
CREATE TABLE NOHOOK1 (name string, number int);
CREATE TABLE NOHOOK2 (name string, number int);
CREATE TABLE NOHOOK3 (name string, number int);
CREATE TABLE NOHOOK4 (name string, number int);

CREATE TABLE HBASEHOOK0 (key int, val binary)
    STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
    WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key,cf:val#b"
);
CREATE TABLE HBASEHOOK1 (key int, val binary)
    STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
    WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key,cf:val#b"
);
CREATE TABLE HBASEHOOK2 (key int, val binary)
    STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
    WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key,cf:val#b"
);

set metastore.batch.retrieve.max=300;
DROP DATABASE sometableshavehook CASCADE;
SHOW DATABASES;