set hive.cbo.enable=false;

drop table if exists dynpart_bucket;
CREATE TABLE dynpart_bucket (bn string) PARTITIONED BY (br string) CLUSTERED BY (bn) INTO 2 BUCKETS ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE ;
INSERT into TABLE dynpart_bucket VALUES ('tv_0', 'tv');
set hive.cbo.enable=true;
INSERT into TABLE dynpart_bucket VALUES ('tv_1', 'tv');
select * from dynpart_bucket;

