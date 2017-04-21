describe function width_bucket;
desc function extended width_bucket;

explain select width_bucket(10, 5, 25, 4);

select
width_bucket(1, 5, 25, 4),
width_bucket(10, 5, 25, 4),
width_bucket(20, 5, 25, 4),
width_bucket(30, 5, 25, 4);

select
width_bucket(1, NULL, 25, 4),
width_bucket(NULL, 5, 25, 4),
width_bucket(20, 5, NULL, 4),
width_bucket(30, 5, 25, NULL),
width_bucket(NULL, NULL, NULL, NULL);

select
width_bucket(-1, -25, -5, 4),
width_bucket(-10, -25, -5, 4),
width_bucket(-20, -25, -5, 4),
width_bucket(-30, -25, -5, 4);

select
width_bucket(-10, -5, 15, 4),
width_bucket(0, -5, 15, 4),
width_bucket(10, -5, 15, 4),
width_bucket(20, -5, 15, 4);
