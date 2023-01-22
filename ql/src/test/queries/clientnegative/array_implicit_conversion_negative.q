drop table if exists aic_tab;
create table aic_tab (c1 array<array<smallint>>);
insert into aic_tab(c1) values (array(55,54));
select * from aic_tab;

