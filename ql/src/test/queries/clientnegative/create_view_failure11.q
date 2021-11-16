-- CREATE VIEW should fail if it references a temp table in a subquery
create temporary table tmp1 (c1 string, c2 string);

create view tmp1_view as
select subq.c1 from (select c1, c2 from tmp1) subq;
