--! qt:transactional


create table sketch_input (id int, category char(1))
STORED AS ORC
TBLPROPERTIES ('transactional'='true');

insert into table sketch_input values
  (1,'a'),(1, 'a'), (2, 'a'), (3, 'a'), (4, 'a'), (5, 'a'), (6, 'a'), (7, 'a'), (8, 'a'), (9, 'a'), (10, 'a'),
  (6,'b'),(6, 'b'), (7, 'b'), (8, 'b'), (9, 'b'), (10, 'b'), (11, 'b'), (12, 'b'), (13, 'b'), (14, 'b'), (15, 'b')
; 

select id,cume_dist() over (order by id) from sketch_input;

set hive.optimize.bi.enabled=true;

SELECT id,11, CUME_DIST() OVER (ORDER BY id),
    ds_quantile_doubles_cdf(ds, CAST(id AS DOUBLE) - 0.01)[0]
    FROM sketch_input JOIN (
      SELECT ds_quantile_doubles_sketch(CAST(id AS DOUBLE)) AS ds FROM sketch_input
    ) q;




-- see if rewrite happens
explain
select id,cume_dist() over (order by id) from sketch_input;

select id,cume_dist() over (order by id) from sketch_input;

