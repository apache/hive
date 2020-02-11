drop table if exists  u_table_4;

create table u_table_4(smallint_col_22 smallint, int_col_5 int);
insert into u_table_4 values(238,922);

drop table u_table_7;
create table u_table_7 ( bigint_col_3 bigint, int_col_10 int);
insert into u_table_7 values (571,198);

drop table u_table_19;
create table u_table_19 (bigint_col_18 bigint ,int_col_19 int, STRING_COL_7 string);
insert into u_table_19 values (922,5,'500');


set hive.mapjoin.full.outer=true;
set hive.auto.convert.join=true;
set hive.query.results.cache.enabled=false;
set hive.merge.nway.joins=true;
set hive.vectorized.execution.enabled=true;
set hive.vectorized.reuse.scratch.columns=true;

explain vectorization detail
SELECT
 	a5.int_col,
  922 as expected,
  COALESCE(a5.int_col, a5.aa) as expected2,
  a5.int_col_3 as reality
FROM            u_table_19 a1 
FULL OUTER JOIN 
                ( 
                       SELECT a2.int_col_5 AS int_col, 
                    				  a2.smallint_col_22 as aa,
                              COALESCE(a2.int_col_5, a2.smallint_col_22) AS int_col_3 
                       FROM   u_table_4 a2
				) a5 
ON              ( 
                                a1.bigint_col_18) = (a5.int_col_3) 
INNER JOIN 
                ( 
                         SELECT   a3.bigint_col_3                                                                                               AS int_col,
                                  Cast (COALESCE(a3.bigint_col_3, a3.bigint_col_3, a3.int_col_10) AS BIGINT) * Cast (a3.bigint_col_3 AS BIGINT) AS int_col_3
                         FROM     u_table_7 a3 
                         WHERE    bigint_col_3=571 
                ) a4
ON              (a1.int_col_19=5) 
OR              ((a5.int_col_3) IN (a4.int_col, 10)) 
where
  a1.STRING_COL_7='500'
ORDER BY        int_col DESC nulls last limit 100
;


SELECT
 	a5.int_col,
  922 as expected,
  COALESCE(a5.int_col, a5.aa) as expected2,
  a5.int_col_3 as reality
FROM            u_table_19 a1 
FULL OUTER JOIN 
                ( 
                       SELECT a2.int_col_5 AS int_col, 
                    				  a2.smallint_col_22 as aa,
                              COALESCE(a2.int_col_5, a2.smallint_col_22) AS int_col_3 
                       FROM   u_table_4 a2
				) a5 
ON              ( 
                                a1.bigint_col_18) = (a5.int_col_3) 
INNER JOIN 
                ( 
                         SELECT   a3.bigint_col_3                                                                                               AS int_col,
                                  Cast (COALESCE(a3.bigint_col_3, a3.bigint_col_3, a3.int_col_10) AS BIGINT) * Cast (a3.bigint_col_3 AS BIGINT) AS int_col_3
                         FROM     u_table_7 a3 
                         WHERE    bigint_col_3=571 
                ) a4
ON              (a1.int_col_19=5) 
OR              ((a5.int_col_3) IN (a4.int_col, 10)) 
where
  a1.STRING_COL_7='500'
ORDER BY        int_col DESC nulls last limit 100
;
