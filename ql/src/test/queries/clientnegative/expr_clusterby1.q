-- expressions are not allowed in cluster by without an alias
explain
select key + key as foo, src.value from src cluster by foo, src.value;

explain
select key + key, src.value from src cluster by key + key, src.value;
