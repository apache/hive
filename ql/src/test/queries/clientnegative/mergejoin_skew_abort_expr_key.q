-- Skew abort with a UDF expression as join key.
-- The skew message should show the expression string (e.g. "upper(val)")
-- rather than an internal column name, because resolveKeyColumnName() falls
-- through to ExprNodeDesc.getExprString() for non-column key expressions.
set hive.explain.user=false;
set hive.auto.convert.join=false;
set hive.merge.join.skew.threshold=2;
set hive.merge.join.skew.abort=true;
set hive.merge.join.skew.check.interval=1;

CREATE TABLE merge_skew_expr_a (id int, val string);
CREATE TABLE merge_skew_expr_b (id int, val string);

-- key UPPER('x') appears 4 times in table a -> triggers abort at threshold=2
INSERT INTO TABLE merge_skew_expr_a VALUES
  (1, 'x'), (2, 'x'), (3, 'x'), (4, 'x'), (5, 'y');
INSERT INTO TABLE merge_skew_expr_b VALUES
  (10, 'x'), (20, 'y');

SELECT a.id, b.id
FROM merge_skew_expr_a a JOIN merge_skew_expr_b b ON UPPER(a.val) = UPPER(b.val);

DROP TABLE merge_skew_expr_a;
DROP TABLE merge_skew_expr_b;

