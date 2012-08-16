-- expressions are not allowed in order by without an alias
explain
select length(value) as foo, src.key from src order by foo, src.key;

explain
select length(value), key from src order by length(value), key;
