-- since CBO doesn't allow such queries we can not support subqueries here
explain select (select max(p_size) from part);
