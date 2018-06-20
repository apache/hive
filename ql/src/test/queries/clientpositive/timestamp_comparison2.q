-- Test timestamp-to-numeric comparison
select count(*) 
FROM   alltypesorc
WHERE  
((ctinyint != 0)
    AND 
        (((ctimestamp1 <= 0) 
            OR ((ctinyint = cint) OR (cstring2 LIKE 'ss')))
         AND ((988888 < cdouble)
             OR ((ctimestamp2 > -29071) AND (3569 >= cdouble)))))
;

-- Should have same result as previous query
select count(*)
FROM   alltypesorc
WHERE  
((ctinyint != 0)
    AND 
        (((ctimestamp1 <= timestamp('1970-01-01 00:00:00'))
            OR ((ctinyint = cint) OR (cstring2 LIKE 'ss')))
         AND ((988888 < cdouble)
             OR ((ctimestamp2 > timestamp('1969-12-31 15:55:29')) AND (3569 >= cdouble)))))
;
