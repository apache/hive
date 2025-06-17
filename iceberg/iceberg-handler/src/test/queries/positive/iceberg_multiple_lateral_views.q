create external table test(id int, arr array<string>) stored by iceberg;
insert into test values (1, array("a", "b")), (2, array("c", "d")), (3, array("e", "f"));

select * from test
lateral view explode(arr) tbl1 as name
lateral view explode(arr) tbl2 as name1;
