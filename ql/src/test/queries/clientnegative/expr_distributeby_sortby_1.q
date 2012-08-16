-- expressions are not allowed in a distribute by or a sort by clause
-- without an alias
explain
select key + key as foo, src.value from src distribute by foo sort by src.value;

explain
select key + key as foo, src.value from src distribute by key + key sort by src.value;
