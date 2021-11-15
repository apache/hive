-- prepare input data
create temporary table sketch_input (id int, category char(1));
insert into table sketch_input values
  (1, 'a'), (2, 'a'), (3, 'a'), (4, 'a'), (5, 'a'), (6, 'a'), (7, 'a'), (8, 'a'), (9, 'a'), (10, 'a'),
  (6, 'b'), (7, 'b'), (8, 'b'), (9, 'b'), (10, 'b'), (11, 'b'), (12, 'b'), (13, 'b'), (14, 'b'), (15, 'b');

-- build sketches per category
create temporary table sketch_intermediate (category char(1), sketch binary);
insert into sketch_intermediate select category, ds_hll_sketch(id) from sketch_input group by category;

-- get unique count estimates per category
select category, ds_hll_estimate(sketch) from sketch_intermediate;


-- union sketches across categories and get overall unique count estimate
select ds_hll_estimate(ds_hll_union(sketch)) from sketch_intermediate;
