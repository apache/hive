-- invalid numbers of arguments
SELECT sort_array_by(array(struct(800 ,'Foo',28,80000) , struct(100,'Boo',21,70000))) FROM src LIMIT 1;