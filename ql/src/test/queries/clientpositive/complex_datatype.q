set hive.default.fileformat=TEXTFILE;
set hive.fetch.task.conversion=none;
set hive.llap.io.enabled=false;
set hive.vectorized.execution.enabled=true;

create EXTERNAL table `complex_map_array_table` as
select
'bob' as name,
  MAP(
      "Key1",
      ARRAY(
        1,
        2,
        3
        ),
      "Key2",
      ARRAY(
        4,
        5,
        6
        )
     ) as column2;

create EXTERNAL table `complex_map_struct_table` as
select
'bob' as name,
MAP(
    "Map_Key1",
    named_struct(
      'Id',
      'Id_Value1',
      'Name',
      'Name_Value1'
      ),
    "Map_Key2",
    named_struct(
      'Id',
      'Id_Value2',
      'Name',
      'Name_Value2'
      )
   ) as column2;


create EXTERNAL table `complex_table1` as
select
MAP(
    "Key1",
    ARRAY(
      1,
      2,
      3
      ),
    "Key2",
    ARRAY(
      4,
      5,
      6
      )
   ) as column1,
'bob' as name,
MAP(
    "Map_Key1",
    named_struct(
      'Id',
      'Id_Value1',
      'Name',
      'Name_Value1'
      ),
    "Map_Key2",
    named_struct(
      'Id',
      'Id_Value2',
      'Name',
      'Name_Value2'
      )
   ) as column3;

create EXTERNAL table `complex_table2` as
select
MAP(
    "Key1",
    ARRAY(
      1,
      2,
      3
      ),
    "Key2",
    ARRAY(
      4,
      5,
      6
      )
   ) as column1,
MAP(
    "Map_Key1",
    named_struct(
      'Id',
      'Id_Value1',
      'Name',
      'Name_Value1'
      ),
    "Map_Key2",
    named_struct(
      'Id',
      'Id_Value2',
      'Name',
      'Name_Value2'
      )
   ) as column2;

create EXTERNAL table `complex_table3` as
select
MAP(
    "Key1",
    ARRAY(
      1,
      2,
      3
      ),
    "Key2",
    ARRAY(
      4,
      5,
      6
      )
   ) as column1,
MAP(
    "Key3",
    ARRAY(
      7,
      8,
      9
      ),
    "Key4",
    ARRAY(
      10,
      11,
      12
      )
   ) as column2;

-- The below scenario's was working before fix
create EXTERNAL table `complex_array_map_table` as
select
'bob' as name,
ARRAY(
    MAP(
      "Key1",
      "Value1"
      ),
    MAP(
      "Key2",
      "Value2"
      )
    ) as column2;

create EXTERNAL table `complex_map_map_table` as
select
  'bob' as name,
  MAP(
    "Key1",
    MAP(
      1,
      2
    ),
    "Key2",
    MAP(
    3,
    4
    )
  ) as column2;

create EXTERNAL table `complex_combined_table` as
select
  ARRAY('arr_val1', 'arr_val2', 'arr_val3') as column1,
  'bob' as column2,
  MAP(
    "Key1",
    ARRAY(
      1,
      2,
      3
      ),
    "Key2",
    ARRAY(
      4,
      5,
      6
      )
   ) as column3,
  NAMED_STRUCT('abc', '7', 'def', '8') as column4,
  MAP(
    "Key3",
    "Value3",
    "Key4",
    "Value4"
    ) as column5;

-- with vectorization set as "true"
select * from complex_map_array_table;
select * from complex_map_struct_table;
select * from complex_table1;
select * from complex_table2;
select * from complex_table3;
select * from complex_array_map_table;
select * from complex_map_map_table;
select * from complex_combined_table;

-- with fetch task conversion set as "more"
set hive.fetch.task.conversion=more;

select * from complex_map_array_table;
select * from complex_map_struct_table;
select * from complex_table1;
select * from complex_table2;
select * from complex_table3;
select * from complex_array_map_table;
select * from complex_map_map_table;
select * from complex_combined_table;

-- with vectorization set as "false"
set hive.vectorized.execution.enabled=false;

select * from complex_map_array_table;
select * from complex_map_struct_table;
select * from complex_table1;
select * from complex_table2;
select * from complex_table3;
select * from complex_array_map_table;
select * from complex_map_map_table;
select * from complex_combined_table;
