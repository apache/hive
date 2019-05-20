SET hive.vectorized.execution.enabled=false;

create table map_txt_n0 (
  id int,
  content map<int,string>
)
row format delimited 
null defined as '\\N'
stored as textfile
;

LOAD DATA LOCAL INPATH '../../data/files/mapNull.txt' INTO TABLE map_txt_n0;

select * from map_txt_n0;

select id, map_keys(content) from map_txt_n0;
