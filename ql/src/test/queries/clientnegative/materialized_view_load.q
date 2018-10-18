create table lmv_basetable (a int, b varchar(256), c decimal(10,2));


create materialized view lmv_mat_view disable rewrite as select a, b, c from lmv_basetable;

LOAD DATA LOCAL INPATH '../../data/files/kv1.txt' INTO TABLE lmv_mat_view;

