-- a struct of two binaries is not a variant, and no SQL route turns one into a variant: CAST(x AS VARIANT) rejects it
CREATE EXTERNAL TABLE variant_from_struct (v VARIANT) STORED BY ICEBERG TBLPROPERTIES ('format-version'='3');
INSERT INTO variant_from_struct SELECT named_struct('metadata', unhex('01'), 'value', unhex('0C01'));
