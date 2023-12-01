create table tab_double(a double);

-- insert some data
LOAD DATA LOCAL INPATH "../../data/files/double.txt" INTO TABLE tab_double;

select count(*) from tab_double;

-- compute statistical summary of data
select compute_stats(a, 'fm', 16) from tab_double;

-- create a partitioned table with a double column

create table tab_double_part(a double) partitioned by (b string);

-- set the skew data flag which will result in the npe; if this is false, the query will run fine
set hive.groupby.skewindata=true;

-- insert some data to a partitioned table
insert overwrite table tab_double_part partition(b='1') select * from tab_double;