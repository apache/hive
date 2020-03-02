
create temporary table theta_input (id int, category char(1));
insert into table theta_input values
  (1, 'a'), (2, 'a'), (3, 'a'), (4, 'a'), (5, 'a'), (6, 'a'), (7, 'a'), (8, 'a'), (9, 'a'), (10, 'a'),
  (6, 'b'), (7, 'b'), (8, 'b'), (9, 'b'), (10, 'b'), (11, 'b'), (12, 'b'), (13, 'b'), (14, 'b'), (15, 'b');

create temporary table sketch_intermediate (category char(1), sketch binary);
insert into sketch_intermediate select category, ds_theta_datatosketch(id) from theta_input group by category;

select category, ds_theta_sketchtoestimate(sketch) from sketch_intermediate;

select ds_theta_sketchtoestimate(ds_theta_unionSketch(sketch)) from sketch_intermediate;

