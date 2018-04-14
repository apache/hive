--! qt:dataset:src
SELECT 'abc' like any ('a%','%d','%cd%')
FROM src WHERE src.key = 86;

SELECT 'abc' LIKE ANY ('z%','%y','%dx%')
FROM src WHERE src.key = 86;

SELECT 'abc' like any ('abc')
FROM src WHERE src.key = 86;

DESCRIBE FUNCTION likeany;
DESCRIBE FUNCTION EXTENDED likeany;

DROP TABLE IF EXISTS like_any_table;

CREATE TABLE like_any_table
STORED AS TEXTFILE
AS
SELECT "google" as company,"%oo%" as pat
UNION ALL
SELECT "facebook" as company,"%oo%" as pat
UNION ALL
SELECT "linkedin" as company,"%in" as pat
;

select company from like_any_table where company like any ('%oo%','%in','fa%') ;

select company from like_any_table where company like any ('microsoft','%yoo%') ;

select
    company,
    CASE
        WHEN company like any ('%oo%','%in','fa%') THEN 'Y'
        ELSE 'N'
    END AS is_available,
    CASE
        WHEN company like any ('%oo%','fa%') OR company like any ('%in','ms%') THEN 'Y'
        ELSE 'N'
    END AS mix
From like_any_table;

--Mix test with constant pattern and column value
select company from like_any_table where company like any ('%zz%',pat) ;

-- not like any test

select company from like_any_table where company not like any ('%oo%','%in','fa%') ;
select company from like_any_table where company not like any ('microsoft','%yoo%') ;

-- null test

select company from like_any_table where company like any ('%oo%',null) ;

select company from like_any_table where company not like any ('%oo%',null) ;

select company from like_any_table where company like any (null,null) ;

select company from like_any_table where company not like any (null,null) ;