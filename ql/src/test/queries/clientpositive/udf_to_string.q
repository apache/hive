--! qt:dataset:alltypesorc
set hive.fetch.task.conversion=more;

-- Conversion of main primitive types to String type:
SELECT CAST(NULL AS STRING);

SELECT CAST(TRUE AS STRING);

SELECT CAST(CAST(1 AS TINYINT) AS STRING);
SELECT CAST(CAST(-18 AS SMALLINT) AS STRING);
SELECT CAST(-129 AS STRING);
SELECT CAST(CAST(-1025 AS BIGINT) AS STRING);

SELECT CAST(CAST(-3.14 AS DOUBLE) AS STRING);
SELECT CAST(CAST(-3.14 AS FLOAT) AS STRING);
SELECT CAST(CAST(-3.14 AS DECIMAL(3,2)) AS STRING);

SELECT CAST('Foo' AS STRING);

SELECT CAST(from_utc_timestamp(timestamp '2018-05-02 15:30:30', 'PST') - from_utc_timestamp(timestamp '1970-01-30 16:00:00', 'PST') AS STRING);
SELECT CAST(interval_year_month('1-2') AS STRING);

-- Conversion of complex types to String type:
select '"' || cast(array(*) as string) || '"' from alltypesorc limit 3;
select '"' || cast(struct(*) as string) || '"' from alltypesorc limit 3;
select '"' || cast(map("ctinyint" , ctinyint, "csmallint" , csmallint, "cint" , cint, "cbigint" , cbigint, "cfloat" , cfloat, "cdouble" , cdouble, "cstring1" , cstring1, "cstring2" , cstring2, "ctimestamp1" , ctimestamp1, "ctimestamp2" , ctimestamp2, "cboolean1" , cboolean1, "cboolean2" , cboolean2) as string) || '"' from alltypesorc limit 3;
select '"' || cast(create_union(if(csmallint % 2 == 0, 0, 1), array(*), struct(*)) as string) || '"' from alltypesorc limit 3;

select '"' || cast(
        struct(
            map("key1", array(csmallint, csmallint+1), "key2", array(csmallint+2, csmallint+3)),
            struct(cstring1, cboolean1, create_union(if(csmallint % 2 == 0, 0, 1), array(*), struct(*)))
        )
    as string) || '"' from alltypesorc limit 3;
