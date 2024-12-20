
set hive.vectorized.execution.enabled=false;
set hive.mapred.mode=nonstrict;
set hive.exec.submitviachild=false;
set hive.exec.submit.local.task.via.child=false;

CREATE TABLE orc_create_people_staging_n0 (
  id int,
  first_name string,
  last_name string,
  address string,
  salary decimal,
  start_date timestamp,
  state string);

LOAD DATA LOCAL INPATH '../../data/files/orc_create_people.txt' OVERWRITE INTO TABLE orc_create_people_staging_n0;


set hive.stats.autogather=false;
-- non-partitioned table
-- partial scan gather
CREATE TABLE orc_create_people_n0 (
  id int,
  first_name string,
  last_name string,
  address string,
  salary decimal,
  start_date timestamp,
  state string)
STORED AS orc;

INSERT OVERWRITE TABLE orc_create_people_n0 SELECT * FROM orc_create_people_staging_n0 ORDER BY id;

set hive.stats.autogather = true;
analyze table orc_create_people_n0 compute statistics;
desc formatted orc_create_people_n0;

analyze table orc_create_people_n0 compute statistics noscan;
desc formatted orc_create_people_n0;

drop table orc_create_people_n0;

-- auto stats gather
CREATE TABLE orc_create_people_n0 (
  id int,
  first_name string,
  last_name string,
  address string,
  salary decimal,
  start_date timestamp,
  state string)
STORED AS orc;

INSERT OVERWRITE TABLE orc_create_people_n0 SELECT * FROM orc_create_people_staging_n0 ORDER BY id;

desc formatted orc_create_people_n0;

drop table orc_create_people_n0;

set hive.stats.autogather=false;
-- partitioned table
-- partial scan gather
CREATE TABLE orc_create_people_n0 (
  id int,
  first_name string,
  last_name string,
  address string,
  salary decimal,
  start_date timestamp)
PARTITIONED BY (state string)
STORED AS orc;

INSERT OVERWRITE TABLE orc_create_people_n0 PARTITION (state)
  SELECT * FROM orc_create_people_staging_n0 ORDER BY id;

set hive.stats.autogather = true;
analyze table orc_create_people_n0 partition(state) compute statistics;
desc formatted orc_create_people_n0 partition(state="Ca");
desc formatted orc_create_people_n0 partition(state="Or");

analyze table orc_create_people_n0 partition(state) compute statistics noscan;
desc formatted orc_create_people_n0 partition(state="Ca");
desc formatted orc_create_people_n0 partition(state="Or");

drop table orc_create_people_n0;

-- auto stats gather
CREATE TABLE orc_create_people_n0 (
  id int,
  first_name string,
  last_name string,
  address string,
  salary decimal,
  start_date timestamp)
PARTITIONED BY (state string)
STORED AS orc;

INSERT OVERWRITE TABLE orc_create_people_n0 PARTITION (state)
  SELECT * FROM orc_create_people_staging_n0 ORDER BY id;

desc formatted orc_create_people_n0 partition(state="Ca");
desc formatted orc_create_people_n0 partition(state="Or");

drop table orc_create_people_n0;

set hive.stats.autogather=false;
-- partitioned and bucketed table
-- partial scan gather
CREATE TABLE orc_create_people_n0 (
  id int,
  first_name string,
  last_name string,
  address string,
  salary decimal,
  start_date timestamp)
PARTITIONED BY (state string)
clustered by (first_name)
sorted by (last_name)
into 4 buckets
STORED AS orc;

INSERT OVERWRITE TABLE orc_create_people_n0 PARTITION (state)
  SELECT * FROM orc_create_people_staging_n0 ORDER BY id;

set hive.stats.autogather = true;
analyze table orc_create_people_n0 partition(state) compute statistics;
desc formatted orc_create_people_n0 partition(state="Ca");
desc formatted orc_create_people_n0 partition(state="Or");

analyze table orc_create_people_n0 partition(state) compute statistics noscan;
desc formatted orc_create_people_n0 partition(state="Ca");
desc formatted orc_create_people_n0 partition(state="Or");

drop table orc_create_people_n0;

-- auto stats gather
CREATE TABLE orc_create_people_n0 (
  id int,
  first_name string,
  last_name string,
  address string,
  salary decimal,
  start_date timestamp)
PARTITIONED BY (state string)
clustered by (first_name)
sorted by (last_name)
into 4 buckets
STORED AS orc;

INSERT OVERWRITE TABLE orc_create_people_n0 PARTITION (state)
  SELECT * FROM orc_create_people_staging_n0 ORDER BY id;

desc formatted orc_create_people_n0 partition(state="Ca");
desc formatted orc_create_people_n0 partition(state="Or");

drop table orc_create_people_n0;

set hive.stats.autogather=false;
-- create table with partitions containing text and ORC files.
-- ORC files implements StatsProvidingRecordReader but text files does not.
-- So the partition containing text file should not have statistics.
CREATE TABLE orc_create_people_n0 (
  id int,
  first_name string,
  last_name string,
  address string,
  salary decimal,
  start_date timestamp)
PARTITIONED BY (state string)
STORED AS orc;

INSERT OVERWRITE TABLE orc_create_people_n0 PARTITION (state)
  SELECT * FROM orc_create_people_staging_n0 ORDER BY id;

set hive.stats.autogather = true;
analyze table orc_create_people_n0 partition(state) compute statistics;
desc formatted orc_create_people_n0 partition(state="Ca");

analyze table orc_create_people_n0 partition(state) compute statistics noscan;
desc formatted orc_create_people_n0 partition(state="Ca");

drop table orc_create_people_n0;
