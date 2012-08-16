-- expressions are not allowed in sort by without an alias
explain
select key - 10 as foo, value from src sort by foo, value;

explain
select key - 10, value from src sort by key - 10, value;
