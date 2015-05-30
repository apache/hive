-- invalid argument type
SELECT sort_array(array(create_union(0,"a"))) FROM src LIMIT 1;
