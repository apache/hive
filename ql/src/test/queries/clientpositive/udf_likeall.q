--! qt:dataset:src
SELECT 'abc' like all ('a%','%bc%','%c')
FROM src WHERE src.key = 86;

SELECT 'abc' LIKE ALL ('z%','%y','%dx%')
FROM src WHERE src.key = 86;

SELECT 'abc' like all ('abc')
FROM src WHERE src.key = 86;

DESCRIBE FUNCTION likeall;
DESCRIBE FUNCTION EXTENDED likeall;

DROP TABLE IF EXISTS like_all_table;

CREATE TABLE like_all_table
STORED AS TEXTFILE
AS
SELECT "google" as company,"%gl%" as pat
UNION ALL
SELECT "facebook" as company,"%bo%" as pat
UNION ALL
SELECT "linkedin" as company,"%in" as pat
;

select company from like_all_table where company like all ('%oo%','%go%') ;

select company from like_all_table where company like all ('microsoft','%yoo%') ;

select
    company,
    CASE
        WHEN company like all ('%oo%','%go%') THEN 'Y'
        ELSE 'N'
    END AS is_available,
    CASE
        WHEN company like all ('%oo%','go%') OR company like all ('%in','ms%') THEN 'Y'
        ELSE 'N'
    END AS mix
From like_all_table ;

--Mix test with constant pattern and column value
select company from like_all_table where company like all ('%oo%',pat) ;

-- not like all test

select company from like_all_table where company not like all ('%oo%','%in','fa%') ;
select company from like_all_table where company not like all ('microsoft','%yoo%') ;

-- null test

select company from like_all_table where company like all ('%oo%',null) ;

select company from like_all_table where company not like all ('%oo%',null) ;

select company from like_all_table where company not like all (null,null) ;

select company from like_all_table where company not like all (null,null) ;