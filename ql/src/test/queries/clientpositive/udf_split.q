EXPLAIN SELECT 
  split('a b c', ' '),
  split('oneAtwoBthreeC', '[ABC]'),
  split('', '.')
FROM src LIMIT 1;

SELECT 
  split('a b c', ' '),
  split('oneAtwoBthreeC', '[ABC]'),
  split('', '.')
FROM src LIMIT 1;
