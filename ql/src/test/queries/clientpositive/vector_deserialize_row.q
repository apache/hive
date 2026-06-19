CREATE external TABLE IF NOT EXISTS sessions
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

CREATE TABLE IF NOT EXISTS sessions_orc
(
session_id string,
uid bigint,
uids array<bigint>,
search_ids array<string>,
total_views int,
datestamp date
);

describe formatted sessions_orc;

INSERT OVERWRITE TABLE sessions_orc
SELECT * FROM sessions;

select count(1) from sessions_orc;
select count(1) from sessions;
drop table sessions;
drop table sessions_orc;

