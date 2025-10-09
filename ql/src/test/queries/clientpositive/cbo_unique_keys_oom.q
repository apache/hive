CREATE TABLE t (
    c1 string,
    c2 string,
    c3 string,
    c4 string,
    c5 string,
    c6 string,
    c7 string,
    c8 string,
    c9 string
);

EXPLAIN CBO
SELECT a0
FROM 
(SELECT c1 as a0,
        c1 as a1,
        c1 as a2,
        c2 as a3,
        c2 as a4,
        c2 as a5,
        c3 as a6,
        c3 as a7,
        c3 as a8,
        c4 as a9,
        c4 as a10,
        c4 as a11,
        c5 as a12,
        c5 as a13,
        c5 as a14,
        c6 as a15,
        c6 as a16,
        c6 as a17,
        c7 as a18,
        c7 as a19,
        c7 as a20,
        c8 as a21,
        c8 as a22,
        c8 as a23,
        c9 as a24,
        c9 as a25,
        c9 as a26
FROM t GROUP BY c1,c2,c3,c4,c5,c6,c7,c8,c9) t1
GROUP BY a0, a4
