create table test_binary(data_col timestamp, binary_col binary) partitioned by (ts string);
insert into test_binary partition(ts='202204200000') values ('2022-04-20 00:00:00.0', 'a'),
('2022-04-20 00:00:00.0', 'b'),('2022-04-20 00:00:00.0', 'c'),('2022-04-20 00:00:00.0', NULL);
select * from test_binary where ts='202204200000' and binary_col = unhex('61');
select * from test_binary where ts='202204200000' and binary_col between unhex('61') and unhex('62');
select * from test_binary where binary_col = unhex('61') or binary_col = unhex('62');
select * from test_binary where ts='202204200000' and (binary_col = unhex('61') or binary_col = unhex('62'));
select * from test_binary where binary_col in (unhex('61'), unhex('62'));
