CREATE TABLE orc_create_people_staging (
  id int,
  first_name string,
  last_name string,
  address string,
  state string);

LOAD DATA LOCAL INPATH '../../data/files/orc_create_people.txt' OVERWRITE INTO TABLE orc_create_people_staging;

set hive.exec.dynamic.partition.mode=nonstrict;

set hive.stats.autogather=false;
-- non-partitioned table
-- partial scan gather
CREATE TABLE orc_create_people (
  id int,
  first_name string,
  last_name string,
  address string,
  state string)
STORED AS orc;

INSERT OVERWRITE TABLE orc_create_people SELECT * FROM orc_create_people_staging;

set hive.stats.autogather = true;
analyze table orc_create_people compute statistics partialscan;

desc formatted orc_create_people;

drop table orc_create_people;

-- auto stats gather
CREATE TABLE orc_create_people (
  id int,
  first_name string,
  last_name string,
  address string,
  state string)
STORED AS orc;

INSERT OVERWRITE TABLE orc_create_people SELECT * FROM orc_create_people_staging;

desc formatted orc_create_people;

drop table orc_create_people;

set hive.stats.autogather=false;
-- partitioned table
-- partial scan gather
CREATE TABLE orc_create_people (
  id int,
  first_name string,
  last_name string,
  address string)
PARTITIONED BY (state string)
STORED AS orc;

INSERT OVERWRITE TABLE orc_create_people PARTITION (state)
  SELECT * FROM orc_create_people_staging;

set hive.stats.autogather = true;
analyze table orc_create_people partition(state) compute statistics partialscan;

desc formatted orc_create_people partition(state="Ca");
desc formatted orc_create_people partition(state="Or");

drop table orc_create_people;

-- auto stats gather
CREATE TABLE orc_create_people (
  id int,
  first_name string,
  last_name string,
  address string)
PARTITIONED BY (state string)
STORED AS orc;

INSERT OVERWRITE TABLE orc_create_people PARTITION (state)
  SELECT * FROM orc_create_people_staging;

desc formatted orc_create_people partition(state="Ca");
desc formatted orc_create_people partition(state="Or");

drop table orc_create_people;

set hive.stats.autogather=false;
-- partitioned and bucketed table
-- partial scan gather
CREATE TABLE orc_create_people (
  id int,
  first_name string,
  last_name string,
  address string)
PARTITIONED BY (state string)
clustered by (first_name)
sorted by (last_name)
into 4 buckets
STORED AS orc;

INSERT OVERWRITE TABLE orc_create_people PARTITION (state)
  SELECT * FROM orc_create_people_staging;

set hive.stats.autogather = true;
analyze table orc_create_people partition(state) compute statistics partialscan;

desc formatted orc_create_people partition(state="Ca");
desc formatted orc_create_people partition(state="Or");

drop table orc_create_people;

-- auto stats gather
CREATE TABLE orc_create_people (
  id int,
  first_name string,
  last_name string,
  address string)
PARTITIONED BY (state string)
clustered by (first_name)
sorted by (last_name)
into 4 buckets
STORED AS orc;

INSERT OVERWRITE TABLE orc_create_people PARTITION (state)
  SELECT * FROM orc_create_people_staging;

desc formatted orc_create_people partition(state="Ca");
desc formatted orc_create_people partition(state="Or");

drop table orc_create_people;

set hive.stats.autogather=false;
-- create table with partitions containing text and ORC files.
-- ORC files implements StatsProvidingRecordReader but text files does not.
-- So the partition containing text file should not have statistics.
CREATE TABLE orc_create_people (
  id int,
  first_name string,
  last_name string,
  address string)
PARTITIONED BY (state string)
STORED AS orc;

INSERT OVERWRITE TABLE orc_create_people PARTITION (state)
  SELECT * FROM orc_create_people_staging;

ALTER TABLE orc_create_people ADD PARTITION(state="OH");
LOAD DATA LOCAL INPATH '../../data/files/kv1.txt' OVERWRITE INTO TABLE orc_create_people PARTITION(state="OH");

set hive.stats.autogather = true;
analyze table orc_create_people partition(state) compute statistics noscan;

desc formatted orc_create_people partition(state="Ca");
desc formatted orc_create_people partition(state="OH");

drop table orc_create_people;
