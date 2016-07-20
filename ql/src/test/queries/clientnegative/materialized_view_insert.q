create table imv_basetable (a int, b varchar(256), c decimal(10,2));


create materialized view imv_mat_view as select a, b, c from imv_basetable;

insert into imv_mat_view values (1, 'fred', 3.14);
