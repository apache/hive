set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION bround;
DESC FUNCTION EXTENDED bround;

select
bround(2.5),
bround(3.5),
bround(2.49),
bround(3.49),
bround(2.51),
bround(3.51);

select
bround(1.25, 1),
bround(1.35, 1),
bround(1.249, 1),
bround(1.349, 1),
bround(1.251, 1),
bround(1.351, 1);

select
bround(-1.25, 1),
bround(-1.35, 1),
bround(-1.249, 1),
bround(-1.349, 1),
bround(-1.251, 1),
bround(-1.351, 1);

select
bround(55.0, -1),
bround(45.0, -1),
bround(54.9, -1),
bround(44.9, -1),
bround(55.1, -1),
bround(45.1, -1);

select
bround(-55.0, -1),
bround(-45.0, -1),
bround(-54.9, -1),
bround(-44.9, -1),
bround(-55.1, -1),
bround(-45.1, -1);
