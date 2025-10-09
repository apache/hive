CREATE TABLE t0 (`title` string);

explain cbo
SELECT x4 from
    (SELECT concat_ws('L4',x3, x3, x3, x3) as x4 from
        (SELECT concat_ws('L3',x2, x2, x2, x2) as x3 from
            (SELECT concat_ws('L2',x1, x1, x1, x1) as x2 from
                (SELECT concat_ws('L1',x0, x0, x0, x0) as x1 from
                    (SELECT concat_ws('L0',title, title, title, title) as x0 from t0) t1) t2) t3) t4) t
WHERE x4 = 'Something';
