-- Test inserting empty rows into dynamic partitioned and list bucketed blobstore tables

SET hive.blobstore.optimizations.enabled=true;

DROP TABLE empty;
DROP TABLE blobstore_dynamic_partitioning;
DROP TABLE blobstore_list_bucketing;

CREATE TABLE empty (
    id int,
    name string,
    dept string,
    pt string,
    dt string,
    hr string);

CREATE TABLE blobstore_dynamic_partitioning (
    id int,
    name string,
    dept string)
PARTITIONED BY (
    pt string,
    dt string,
    hr string)
LOCATION '${hiveconf:test.blobstore.path.unique}/insert_empty_into_blobstore/blobstore_dynamic_partitioning';

INSERT INTO TABLE blobstore_dynamic_partitioning PARTITION (pt='a', dt, hr) SELECT id, name, dept, dt, hr FROM empty;

SELECT COUNT(*) FROM blobstore_dynamic_partitioning;

CREATE TABLE blobstore_list_bucketing (
    id int,
    name string,
    dept string)
PARTITIONED BY (
    pt string,
    dt string,
    hr string)
SKEWED BY (id) ON ('1', '2', '3') STORED AS DIRECTORIES
LOCATION '${hiveconf:test.blobstore.path.unique}/insert_empty_into_blobstore/blobstore_list_bucketing';

INSERT INTO TABLE blobstore_list_bucketing PARTITION (pt='a', dt='a', hr='a') SELECT id, name, dept FROM empty;

SELECT COUNT(*) FROM blobstore_list_bucketing;

-- Now test empty inserts with blobstore optimizations turned off. This should give us same results.
SET hive.blobstore.optimizations.enabled=false;

INSERT INTO TABLE blobstore_dynamic_partitioning PARTITION (pt='b', dt, hr) SELECT id, name, dept, dt, hr FROM empty;
SELECT COUNT(*) FROM blobstore_dynamic_partitioning;

INSERT INTO TABLE blobstore_list_bucketing PARTITION (pt='b', dt='b', hr='b') SELECT id, name, dept FROM empty;
SELECT COUNT(*) FROM blobstore_list_bucketing;
