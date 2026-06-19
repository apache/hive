DESCRIBE FUNCTION cbrt;
DESC FUNCTION EXTENDED cbrt;

explain select cbrt(27.0);

select
cbrt(0.0),
cbrt(1.0),
cbrt(-1),
cbrt(27),
cbrt(-27.0),
cbrt(87860583272930481),
cbrt(cast(null as double));