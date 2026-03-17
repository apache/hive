CREATE TABLE main_table (
    id_col STRING,
    join_key STRING,
    int_col_to_test INT,
    misc_attr STRING
);

CREATE TABLE lookup_table (
    lookup_key STRING,
    replacement_val INT
);

-- Test search to disjuncts conversion when search is in a Project
EXPLAIN CBO
SELECT
    CASE
        WHEN base.int_col_to_test IS NULL
             OR base.int_col_to_test = ''
             OR base.int_col_to_test = 0
        THEN COALESCE(lkup.replacement_val, 0)
        ELSE base.int_col_to_test
    END AS final_calculated_column
FROM main_table base
JOIN lookup_table lkup ON base.join_key = lkup.lookup_key
;
