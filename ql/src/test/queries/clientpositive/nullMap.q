create table map_txt (
  id int,
  content map<int,string>
)
row format delimited 
null defined as '\\N'
stored as textfile
;

LOAD DATA LOCAL INPATH '../../data/files/mapNull.txt' INTO TABLE map_txt;

select * from map_txt;

select id, map_keys(content) from map_txt;
