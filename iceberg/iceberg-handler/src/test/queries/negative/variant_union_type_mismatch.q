-- UNION ALL of a variant and a non-variant has no common type. Hive rejects it before Calcite;
-- Calcite's leastRestrictive is order-dependent with VARIANT ([VARIANT, INT] -> INT, [INT, VARIANT] -> VARIANT)
CREATE EXTERNAL TABLE variant_union (id INT, v VARIANT) STORED BY ICEBERG TBLPROPERTIES ('format-version'='3');
SELECT v FROM variant_union UNION ALL SELECT id FROM variant_union;
