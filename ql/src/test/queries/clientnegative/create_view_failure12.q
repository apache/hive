-- CREATE VIEW should fail if it joins temp tables
create temporary table tmp1 (c1 string, c2 string);
create temporary table tmp2 (c1 string, c2 string);

create view tmp1_view as
select tmp1.c1 from tmp1
join tmp2 on tmp1.c1 = tmp2.c1;
