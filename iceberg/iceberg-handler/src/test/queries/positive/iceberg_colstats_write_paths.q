-- what each kind of write leaves describing the table
set hive.explain.user=false;
-- a fetch task prints no statistics, and it is the statistics this is about
set hive.fetch.task.conversion=none;
set hive.stats.autogather=true;
set hive.stats.column.autogather=true;
set hive.iceberg.stats.collect.partlevel=true;

create external table ice_src (id bigint, p string)
stored by iceberg tblproperties ('format-version'='2');
insert into ice_src values (1, 'a'), (7, 'b');

-- a CREATE TABLE AS reads every row it writes, so it describes all of them
create external table ice_ctas
stored by iceberg tblproperties ('format-version'='2') as select * from ice_src;

explain select id from ice_ctas where id > 0;

drop table ice_ctas;

-- an insert gathers as it writes, so an unpartitioned table stays described
create external table ice_unpart_w (id bigint, p string)
stored by iceberg tblproperties ('format-version'='2');

insert into ice_unpart_w values (1, 'a'), (7, 'b');
analyze table ice_unpart_w compute statistics for columns;

explain select id from ice_unpart_w where id > 0;

insert into ice_unpart_w values (9, 'c');

explain select id from ice_unpart_w where id > 0;

drop table ice_unpart_w;

-- an overwrite of the whole table replaces the rows and what described them alike
create external table ice_iow (id bigint, p string)
    partitioned by spec (p)
stored by iceberg tblproperties ('format-version'='2');

insert into ice_iow values (1, 'a'), (7, 'b');
analyze table ice_iow compute statistics for columns;

explain select id from ice_iow where id > 0;

insert overwrite table ice_iow select id + 100, p from ice_src;

explain select id from ice_iow where id > 0;

drop table ice_iow;
drop table ice_src;
