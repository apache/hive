create table explain_multiple_ptf_big_table
(
    key1       string,
    value_str1 string,
    key3       string
);
create table explain_multiple_ptf_big_table2
(
    key21       string,
    value_str21 string,
    key23       string
);
alter table explain_multiple_ptf_big_table
    update statistics set('numRows' = '4611686036854775807',
                          'rawDataSize' = '922337203685477500');
alter table explain_multiple_ptf_big_table2
    update statistics set('numRows' = '4611686036854775807',
                          'rawDataSize' = '9223372036854775800');
explain
select *,
       row_number() over (partition by key order by key2 desc)   rn,
       row_number() over (partition by key2 order by key desc)   rn2,
       max(value_str) over (partition by key2 order by key desc) max1,
       max(value_str) over (partition by key order by key2 desc) max3,
       min(value_str) over (partition by key2 order by key desc) min1,
       min(value_str) over (partition by key order by key2 desc) min3,
       last_value(value_str) over (partition by key)             lv,
       first_value(value_str) over (partition by key2)           fv,
       max(value_str) over (partition by key)                    fv21
from (select key1 key, value_str1 value_str, key3 key2
      from explain_multiple_ptf_big_table
      union all
      select key21 key, value_str21 value_str, key23 key2
      from explain_multiple_ptf_big_table2) a1;

