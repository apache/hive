--! qt:dataset:src
set hive.map.aggr=false;
set hive.stats.column.autogather=true;

CREATE TABLE DEST1_n52(key INT, value STRING) STORED AS TEXTFILE;

create table s_n129 as select * from src where key='10';

explain FROM S_n129
INSERT OVERWRITE TABLE DEST1_n52 SELECT key, sum(SUBSTR(value,5)) GROUP BY key
;

FROM S_n129
INSERT OVERWRITE TABLE DEST1_n52 SELECT key, sum(SUBSTR(value,5)) GROUP BY key
;

desc formatted DEST1_n52;

desc formatted DEST1_n52 key;
desc formatted DEST1_n52 value;
