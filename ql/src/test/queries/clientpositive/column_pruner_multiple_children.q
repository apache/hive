set hive.map.aggr=false;
set hive.stats.column.autogather=true;

CREATE TABLE DEST1(key INT, value STRING) STORED AS TEXTFILE;

create table s as select * from src where key='10';

explain FROM S
INSERT OVERWRITE TABLE DEST1 SELECT key, sum(SUBSTR(value,5)) GROUP BY key
;

FROM S
INSERT OVERWRITE TABLE DEST1 SELECT key, sum(SUBSTR(value,5)) GROUP BY key
;

desc formatted DEST1;

desc formatted DEST1 key;
desc formatted DEST1 value;
