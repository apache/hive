-- Test inner join query

DROP TABLE events;
DROP TABLE profiled_users;
DROP TABLE page_profiles_latest;
DROP TABLE page_profiles_out;
CREATE TABLE events (
    userUid string,
    trackingId string,
    eventType string,
    action string,
    url string)
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '
LOCATION '${hiveconf:test.blobstore.path.unique}/join/page-profiles';

LOAD DATA LOCAL INPATH '../../data/files/5col_data.txt' OVERWRITE INTO TABLE events
PARTITION (dt='2010-12-08');

CREATE TABLE profiled_users (
    userUid string,
    categoryId int,
    score bigint,
    count bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '
LINES TERMINATED BY '\n'
LOCATION '${hiveconf:test.blobstore.path.unique}/join/measured-profiles';

LOAD DATA LOCAL INPATH '../../data/files/4col_data.txt' OVERWRITE INTO TABLE profiled_users;

CREATE EXTERNAL TABLE page_profiles_latest (
    url string,
    categoryId int,
    score bigint,
    count bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '
LINES TERMINATED BY '\n'
LOCATION '${hiveconf:test.blobstore.path.unique}/join/page-profiles/dt=2010-12-08';

CREATE TABLE page_profiles_out (
    url string,
    categoryId int,
    score bigint,
    count bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '
LINES TERMINATED BY '\n'
LOCATION '${hiveconf:test.blobstore.path.unique}/join/page_profiles_out';

INSERT OVERWRITE TABLE page_profiles_out
SELECT url, categoryId, SUM(score) as score, SUM(count) AS count
FROM (
    SELECT
        e.url AS url,
        u.categoryId AS categoryId,
        ROUND(SUM(IF(u.score > 0, log2(u.score + 2), 0))) AS score,
        SUM(u.count) AS count
    FROM events e
    JOIN profiled_users u ON (e.userUid = u.userUid)
    WHERE e.userUid != "0"
    GROUP BY e.url, u.categoryId
    UNION ALL
    SELECT * FROM page_profiles_latest
) page_profiles
GROUP BY url, categoryId;

SELECT * FROM page_profiles_out;