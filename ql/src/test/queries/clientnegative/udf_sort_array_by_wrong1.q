-- invalid argument type for first argument
SELECT sort_array_by(array(2, 5, 4),'col1') FROM src LIMIT 1;