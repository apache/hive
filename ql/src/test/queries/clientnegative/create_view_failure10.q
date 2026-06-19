-- CREATE VIEW should fail if it references a temp table
create temporary table tmp1 (c1 string, c2 string);
create view tmp1_view as select c1, count(*) from tmp1 group by c1;
