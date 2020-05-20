CREATE external TABLE IF NOT EXISTS sessions_cloudera
(
session_id string,
uid bigint,
uids array<bigint>,
search_ids array<string>,
total_views int,
datestamp date
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '../../data/files/arrayofIntdata';

CREATE TABLE IF NOT EXISTS sessions_cloudera_orc
(
session_id string,
uid bigint,
uids array<bigint>,
search_ids array<string>,
total_views int,
datestamp date
);

describe formatted sessions_cloudera_orc;

INSERT OVERWRITE TABLE sessions_cloudera_orc
SELECT * FROM sessions_cloudera;

select count(1) from sessions_cloudera_orc;
select count(1) from sessions_cloudera;
