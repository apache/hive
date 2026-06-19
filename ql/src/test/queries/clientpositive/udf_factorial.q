DESCRIBE FUNCTION factorial;
DESC FUNCTION EXTENDED factorial;

explain select factorial(5);

select
factorial(5),
factorial(0),
factorial(20),
factorial(-1),
factorial(21),
factorial(cast(null as int));