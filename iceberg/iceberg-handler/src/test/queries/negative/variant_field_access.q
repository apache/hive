-- a variant has no fields; its metadata/value encoding is not addressable
CREATE EXTERNAL TABLE variant_dot (id INT, v VARIANT) STORED BY ICEBERG TBLPROPERTIES ('format-version'='3');
SELECT v.metadata FROM variant_dot;
