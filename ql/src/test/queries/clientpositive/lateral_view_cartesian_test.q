-- HIVE-29084: LATERAL VIEW cartesian product schema construction
DROP TABLE IF EXISTS src_lv_cart;
CREATE TABLE src_lv_cart (val_array array<string>);
INSERT INTO src_lv_cart VALUES (array('a', 'b'));

SELECT first_val, second_val
FROM src_lv_cart
LATERAL VIEW explode(val_array) lv1 AS first_val
LATERAL VIEW explode(val_array) lv2 AS second_val
WHERE first_val != second_val
ORDER BY first_val, second_val;

SELECT first_val, second_val
FROM (SELECT array('a', 'b') AS val_array) src
LATERAL VIEW explode(val_array) lv1 AS first_val
LATERAL VIEW explode(val_array) lv2 AS second_val
WHERE first_val != second_val
ORDER BY first_val, second_val;

SELECT lv1.val1, lv2.val2, lv3.val3
FROM (SELECT array('x', 'y') AS arr) src
LATERAL VIEW explode(arr) lv1 AS val1
LATERAL VIEW explode(arr) lv2 AS val2
LATERAL VIEW explode(arr) lv3 AS val3
WHERE lv1.val1 != lv2.val2 AND lv2.val2 != lv3.val3
ORDER BY lv1.val1, lv2.val2, lv3.val3;

SELECT first_val, second_val
FROM (SELECT array('p', 'q', 'r') AS val_array) src
LATERAL VIEW explode(val_array) lv1 AS first_val
LATERAL VIEW explode(val_array) lv2 AS second_val
WHERE first_val = 'p' OR second_val = 'r'
ORDER BY first_val, second_val;

SET hive.cbo.enable=false;
SELECT first_val, second_val
FROM (SELECT array('m', 'n') AS val_array) src
LATERAL VIEW explode(val_array) lv1 AS first_val
LATERAL VIEW explode(val_array) lv2 AS second_val
WHERE first_val != second_val
ORDER BY first_val, second_val;

SET hive.cbo.enable=true;
SET hive.optimize.ppd=false;
SELECT first_val, second_val
FROM (SELECT array('s', 't') AS val_array) src
LATERAL VIEW explode(val_array) lv1 AS first_val
LATERAL VIEW explode(val_array) lv2 AS second_val
WHERE first_val != second_val
ORDER BY first_val, second_val;

SET hive.cbo.enable=false;
SET hive.optimize.ppd=false;
SELECT first_val, second_val
FROM (SELECT array('u', 'v') AS val_array) src
LATERAL VIEW explode(val_array) lv1 AS first_val
LATERAL VIEW explode(val_array) lv2 AS second_val
WHERE first_val != second_val
ORDER BY first_val, second_val;

SET hive.cbo.enable=true;
SET hive.optimize.ppd=true;
SELECT outer_val, inner_val1, inner_val2
FROM (
  SELECT array('alpha', 'beta') AS outer_array, array('1', '2') AS inner_array
) src
LATERAL VIEW explode(outer_array) lv_outer AS outer_val
LATERAL VIEW explode(inner_array) lv_inner1 AS inner_val1
LATERAL VIEW explode(inner_array) lv_inner2 AS inner_val2
WHERE outer_val != 'alpha' OR (inner_val1 != inner_val2)
ORDER BY outer_val, inner_val1, inner_val2;

EXPLAIN CBO SELECT first_val, second_val
FROM (SELECT array('a', 'b') AS val_array) src
LATERAL VIEW explode(val_array) lv1 AS first_val
LATERAL VIEW explode(val_array) lv2 AS second_val
WHERE first_val != second_val;
SELECT val1, val2
FROM (SELECT array('test', null, 'data') AS arr_with_nulls) src
LATERAL VIEW explode(arr_with_nulls) lv1 AS val1
LATERAL VIEW explode(arr_with_nulls) lv2 AS val2
WHERE val1 IS NOT NULL AND val2 IS NOT NULL AND val1 != val2
ORDER BY val1, val2;

SELECT num1, num2
FROM (SELECT array(1, 2, 3) AS num_array) src
LATERAL VIEW explode(num_array) lv1 AS num1
LATERAL VIEW explode(num_array) lv2 AS num2
WHERE num1 < num2
ORDER BY num1, num2;

SELECT dec1, dec2
FROM (SELECT array(1.5, 2.7, 3.9) AS dec_array) src
LATERAL VIEW explode(dec_array) lv1 AS dec1
LATERAL VIEW explode(dec_array) lv2 AS dec2
WHERE dec1 != dec2
ORDER BY dec1, dec2;

SELECT bool1, bool2
FROM (SELECT array(true, false) AS bool_array) src
LATERAL VIEW explode(bool_array) lv1 AS bool1
LATERAL VIEW explode(bool_array) lv2 AS bool2
WHERE bool1 != bool2
ORDER BY bool1, bool2;

SELECT pos1, val1, pos2, val2
FROM (SELECT array('x', 'y', 'z') AS test_array) src
LATERAL VIEW posexplode(test_array) lv1 AS pos1, val1
LATERAL VIEW posexplode(test_array) lv2 AS pos2, val2
WHERE pos1 != pos2 AND val1 != val2
ORDER BY pos1, pos2;

SELECT id1, name1, id2, name2
FROM (SELECT array(struct(1, 'Alice'), struct(2, 'Bob'), struct(3, 'Charlie')) AS struct_array) src
LATERAL VIEW inline(struct_array) lv1 AS id1, name1
LATERAL VIEW inline(struct_array) lv2 AS id2, name2
WHERE id1 != id2
ORDER BY id1, id2;

SELECT val1, val2
FROM (SELECT array('a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7', 'a8', 'a9', 'a10') AS large_array) src
LATERAL VIEW explode(large_array) lv1 AS val1
LATERAL VIEW explode(large_array) lv2 AS val2
WHERE val1 < val2 AND (val1 LIKE '%1' OR val1 LIKE '%5')
ORDER BY val1, val2 LIMIT 10;

SELECT key1, value1, key2, value2
FROM (SELECT array(map('k1', 'v1', 'k2', 'v2'), map('k3', 'v3', 'k4', 'v4')) AS map_array) src
LATERAL VIEW explode(map_array) lv1 AS map1
LATERAL VIEW explode(map1) lv_map1 AS key1, value1
LATERAL VIEW explode(map_array) lv2 AS map2
LATERAL VIEW explode(map2) lv_map2 AS key2, value2
WHERE key1 != key2
ORDER BY key1, key2 LIMIT 8;

SELECT val, COUNT(*) AS cnt
FROM (SELECT array('group1', 'group2', 'group1', 'group3') AS grp_array) src
LATERAL VIEW explode(grp_array) lv AS val
GROUP BY val
ORDER BY val;

SELECT val, ROW_NUMBER() OVER (ORDER BY val) AS rn
FROM (SELECT array('w1', 'w2', 'w3', 'w1') AS win_array) src
LATERAL VIEW explode(win_array) lv AS val
ORDER BY rn;

SELECT outer_result
FROM (
  SELECT CONCAT(inner_val, '_processed') AS outer_result
  FROM (SELECT array('sub1', 'sub2') AS sub_array) inner_t
  LATERAL VIEW explode(sub_array) inner_lv AS inner_val
) src
ORDER BY outer_result;

CREATE TEMPORARY TABLE tmp_join (id int, data string, join_array array<string>);
INSERT INTO tmp_join VALUES (1, 'data1', array('join_a', 'join_b')), (2, 'data2', array('join_c', 'join_d'));

SELECT t.id, t.data, lv.exploded_val
FROM tmp_join t
LATERAL VIEW explode(t.join_array) lv AS exploded_val
WHERE t.id = 1
ORDER BY t.id, lv.exploded_val;

EXPLAIN CBO SELECT lv1.val1, lv2.val2, lv3.val3
FROM (SELECT array('x', 'y') AS arr) src
LATERAL VIEW explode(arr) lv1 AS val1
LATERAL VIEW explode(arr) lv2 AS val2
LATERAL VIEW explode(arr) lv3 AS val3
WHERE lv1.val1 != lv2.val2 AND lv2.val2 != lv3.val3;

DROP TABLE src_lv_cart;
DROP TABLE tmp_join;
