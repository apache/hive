create external table ice_t (id int) stored by iceberg;
insert into ice_t  values (1), (2), (3), (4);
explain extended select * from default.ice_t.snapshots;;
select `ice_t.snapshots`.operation from default.ice_t.snapshots;