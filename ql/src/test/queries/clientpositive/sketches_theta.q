-- see here: https://datasketches.apache.org/docs/Theta/ThetaHiveUDFs.html

create temporary table theta_input (id int, category char(1));
insert into table theta_input values
  (1, 'a'), (2, 'a'), (3, 'a'), (4, 'a'), (5, 'a'), (6, 'a'), (7, 'a'), (8, 'a'), (9, 'a'), (10, 'a'),
  (6, 'b'), (7, 'b'), (8, 'b'), (9, 'b'), (10, 'b'), (11, 'b'), (12, 'b'), (13, 'b'), (14, 'b'), (15, 'b');

create temporary table sketch_intermediate (category char(1), sketch binary);
insert into sketch_intermediate select category, ds_theta_sketch(id) from theta_input group by category;

select category, ds_theta_estimate(sketch) from sketch_intermediate;

select ds_theta_estimate(ds_theta_union(sketch)) from sketch_intermediate;



create temporary table sketch_input (id1 int, id2 int);
insert into table sketch_input values
  (1, 2), (2, 4), (3, 6), (4, 8), (5, 10), (6, 12), (7, 14), (8, 16), (9, 18), (10, 20);

create temporary table sketch_intermediate2 (sketch1 binary, sketch2 binary);

insert into sketch_intermediate2 select ds_theta_sketch(id1), ds_theta_sketch(id2) from sketch_input;

select
  ds_theta_estimate(sketch1),
  ds_theta_estimate(sketch2),
  ds_theta_estimate(ds_theta_union_f(sketch1, sketch2)),
  ds_theta_estimate(ds_theta_intersect_f(sketch1, sketch2)),
  ds_theta_estimate(ds_theta_exclude(sketch1, sketch2)),
  ds_theta_estimate(ds_theta_exclude(sketch2, sketch1))
from sketch_intermediate2;

