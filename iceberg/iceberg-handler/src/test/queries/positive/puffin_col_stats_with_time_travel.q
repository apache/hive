set hive.fetch.task.conversion=none;

create external table default.tbl_ice_puffin_time_travel(a int, b string, c int) stored by iceberg;
insert into default.tbl_ice_puffin_time_travel values (1, 'one', 50), (2, 'two', 51);
alter table default.tbl_ice_puffin_time_travel create tag checkpoint;

explain select * from default.tbl_ice_puffin_time_travel;
explain select * from default.tbl_ice_puffin_time_travel.tag_checkpoint;

insert into tbl_ice_puffin_time_travel values
(null, null, null),
(null, null, null),
(null, null, null),
(null, null, null),
(null, null, null),
(null, null, null),
(null, null, null),
(null, null, null),
(null, null, null),
(null, null, null),
(null, null, null),
(null, null, null);

explain select * from default.tbl_ice_puffin_time_travel;
explain select * from default.tbl_ice_puffin_time_travel.tag_checkpoint;
