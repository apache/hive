set hive.cbo.fallback.strategy=NEVER;
CREATE TABLE customer(orders array<string>);
INSERT INTO customer VALUES (ARRAY('0', '1'));

-- EXPLAIN logical is the only explain version that should work at the moment.
-- It is very expensive to compute and display the plan for existing explain variants since
-- operators need to be visited an exponential number of times.
EXPLAIN LOGICAL SELECT c1
FROM customer
lateral view explode(orders) v as c1
lateral view explode(orders) v as c2
lateral view explode(orders) v as c3
lateral view explode(orders) v as c4
lateral view explode(orders) v as c5
lateral view explode(orders) v as c6
lateral view explode(orders) v as c7
lateral view explode(orders) v as c8
lateral view explode(orders) v as c9
lateral view explode(orders) v as c10
lateral view explode(orders) v as c11
lateral view explode(orders) v as c12
lateral view explode(orders) v as c13
lateral view explode(orders) v as c14
lateral view explode(orders) v as c15
lateral view explode(orders) v as c16
lateral view explode(orders) v as c17
lateral view explode(orders) v as c18
lateral view explode(orders) v as c19
lateral view explode(orders) v as c20
lateral view explode(orders) v as c21
lateral view explode(orders) v as c22
lateral view explode(orders) v as c23
lateral view explode(orders) v as c24
lateral view explode(orders) v as c25
lateral view explode(orders) v as c26
lateral view explode(orders) v as c27
lateral view explode(orders) v as c28
lateral view explode(orders) v as c29
lateral view explode(orders) v as c30
lateral view explode(orders) v as c31
lateral view explode(orders) v as c32
lateral view explode(orders) v as c33
lateral view explode(orders) v as c34
lateral view explode(orders) v as c35
lateral view explode(orders) v as c36
lateral view explode(orders) v as c37
lateral view explode(orders) v as c38
lateral view explode(orders) v as c39
lateral view explode(orders) v as c40
lateral view explode(orders) v as c41
lateral view explode(orders) v as c42
lateral view explode(orders) v as c43
lateral view explode(orders) v as c44
lateral view explode(orders) v as c45
lateral view explode(orders) v as c46
lateral view explode(orders) v as c47
lateral view explode(orders) v as c48
lateral view explode(orders) v as c49
lateral view explode(orders) v as c50
;

-- The size of the result set is exponential and depends on the size of the array
-- and the number of lateral views: ARRAY^LATERAL.
-- For array of size 2, and 50 lateral views the result set has 2^50 elements, which is huge.
-- Without a limit clause computing and displaying the result would take forever.
SELECT *
FROM customer
lateral view explode(orders) v as c1
lateral view explode(orders) v as c2
lateral view explode(orders) v as c3
lateral view explode(orders) v as c4
lateral view explode(orders) v as c5
lateral view explode(orders) v as c6
lateral view explode(orders) v as c7
lateral view explode(orders) v as c8
lateral view explode(orders) v as c9
lateral view explode(orders) v as c10
lateral view explode(orders) v as c11
lateral view explode(orders) v as c12
lateral view explode(orders) v as c13
lateral view explode(orders) v as c14
lateral view explode(orders) v as c15
lateral view explode(orders) v as c16
lateral view explode(orders) v as c17
lateral view explode(orders) v as c18
lateral view explode(orders) v as c19
lateral view explode(orders) v as c20
lateral view explode(orders) v as c21
lateral view explode(orders) v as c22
lateral view explode(orders) v as c23
lateral view explode(orders) v as c24
lateral view explode(orders) v as c25
lateral view explode(orders) v as c26
lateral view explode(orders) v as c27
lateral view explode(orders) v as c28
lateral view explode(orders) v as c29
lateral view explode(orders) v as c30
lateral view explode(orders) v as c31
lateral view explode(orders) v as c32
lateral view explode(orders) v as c33
lateral view explode(orders) v as c34
lateral view explode(orders) v as c35
lateral view explode(orders) v as c36
lateral view explode(orders) v as c37
lateral view explode(orders) v as c38
lateral view explode(orders) v as c39
lateral view explode(orders) v as c40
lateral view explode(orders) v as c41
lateral view explode(orders) v as c42
lateral view explode(orders) v as c43
lateral view explode(orders) v as c44
lateral view explode(orders) v as c45
lateral view explode(orders) v as c46
lateral view explode(orders) v as c47
lateral view explode(orders) v as c48
lateral view explode(orders) v as c49
lateral view explode(orders) v as c50
LIMIT 1024;
