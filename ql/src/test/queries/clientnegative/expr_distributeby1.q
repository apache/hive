-- expressions are not allowed in distribute by without an alias
explain
select key, length(value) as foo from src distribute by key, foo;

explain
select key, length(value) from src distribute by key, length(value);
