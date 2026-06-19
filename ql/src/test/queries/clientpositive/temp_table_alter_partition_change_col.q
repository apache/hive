set hive.metastore.disallow.incompatible.col.type.changes=false;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

-- SORT_QUERY_RESULTS

create temporary table alter_partition_change_col0 (c1 string, c2 string);
load data local inpath '../../data/files/dec.txt' overwrite into table alter_partition_change_col0;

create temporary table alter_partition_change_col1 (c1 string, c2 string) partitioned by (p1 string comment 'Column p1', p2 string comment 'Column p2');

insert overwrite table alter_partition_change_col1 partition (p1, p2)
  select c1, c2, 'abc', '123' from alter_partition_change_col0
  union all
  select c1, c2, cast(null as string), '123' from alter_partition_change_col0;

show partitions alter_partition_change_col1;
select * from alter_partition_change_col1 where p1='abc';
select * from alter_partition_change_col1 where p1='__HIVE_DEFAULT_PARTITION__';
select * from alter_partition_change_col1 where p1='__HIVE_DEFAULT_PARTITION__' or lower(p1)='a';

-- Change c2 to decimal(10,0)
alter table alter_partition_change_col1 change c2 c2 decimal(10,0);
alter table alter_partition_change_col1 partition (p1='abc', p2='123') change c2 c2 decimal(10,0);
alter table alter_partition_change_col1 partition (p1='__HIVE_DEFAULT_PARTITION__', p2='123') change c2 c2 decimal(10,0);
select * from alter_partition_change_col1 where p1='abc';
select * from alter_partition_change_col1 where p1='__HIVE_DEFAULT_PARTITION__';

-- Change the column type at the table level. Table-level describe shows the new type, but the existing partition does not.
alter table alter_partition_change_col1 change c2 c2 decimal(14,4);
describe alter_partition_change_col1;
describe alter_partition_change_col1 partition (p1='abc', p2='123');
select * from alter_partition_change_col1 where p1='abc';
select * from alter_partition_change_col1 where p1='__HIVE_DEFAULT_PARTITION__';

-- change the comment on a partition column without changing type or renaming it
explain alter table alter_partition_change_col1 partition column (p1 string comment 'Changed comment for p1');
alter table alter_partition_change_col1 partition column (p1 string comment 'Changed comment for p1');
describe alter_partition_change_col1;

-- now change the column type of the existing partition
alter table alter_partition_change_col1 partition (p1='abc', p2='123') change c2 c2 decimal(14,4);
describe alter_partition_change_col1 partition (p1='abc', p2='123');
select * from alter_partition_change_col1 where p1='abc';
select * from alter_partition_change_col1 where p1='__HIVE_DEFAULT_PARTITION__';

-- change column for default partition value
alter table alter_partition_change_col1 partition (p1='__HIVE_DEFAULT_PARTITION__', p2='123') change c2 c2 decimal(14,4);
describe alter_partition_change_col1 partition (p1='__HIVE_DEFAULT_PARTITION__', p2='123');
select * from alter_partition_change_col1 where p1='abc';
select * from alter_partition_change_col1 where p1='__HIVE_DEFAULT_PARTITION__';

-- Try out replace columns
alter table alter_partition_change_col1 partition (p1='abc', p2='123') replace columns (c1 string);
describe alter_partition_change_col1;
describe alter_partition_change_col1 partition (p1='abc', p2='123');
select * from alter_partition_change_col1 where p1='abc';
select * from alter_partition_change_col1 where p1='__HIVE_DEFAULT_PARTITION__';

alter table alter_partition_change_col1 replace columns (c1 string);
describe alter_partition_change_col1;
select * from alter_partition_change_col1 where p1='abc';
select * from alter_partition_change_col1 where p1='__HIVE_DEFAULT_PARTITION__';

-- Try add columns
alter table alter_partition_change_col1 add columns (c2 decimal(14,4));
describe alter_partition_change_col1;
describe alter_partition_change_col1 partition (p1='abc', p2='123');
select * from alter_partition_change_col1 where p1='abc';
select * from alter_partition_change_col1 where p1='__HIVE_DEFAULT_PARTITION__';

alter table alter_partition_change_col1 partition (p1='abc', p2='123') add columns (c2 decimal(14,4));
describe alter_partition_change_col1 partition (p1='abc', p2='123');
select * from alter_partition_change_col1 where p1='abc';
select * from alter_partition_change_col1 where p1='__HIVE_DEFAULT_PARTITION__';

-- Try changing column for all partitions at once
alter table alter_partition_change_col1 partition (p1, p2='123') change column c2 c2 decimal(10,0);
describe alter_partition_change_col1 partition (p1='abc', p2='123');
describe alter_partition_change_col1 partition (p1='__HIVE_DEFAULT_PARTITION__', p2='123');
select * from alter_partition_change_col1 where p1='abc';
select * from alter_partition_change_col1 where p1='__HIVE_DEFAULT_PARTITION__';

CREATE temporary TABLE `alterPartTbl`(
  col_1col_1col_1col_1col_1col_11 string,
  col_1col_1col_1col_1col_1col_12 string,
  col_1col_1col_1col_1col_1col_13 string,
  col_1col_1col_1col_1col_1col_14 string,
  col_1col_1col_1col_1col_1col_15 string,
  col_1col_1col_1col_1col_1col_16 string,
  col_1col_1col_1col_1col_1col_17 string,
  col_1col_1col_1col_1col_1col_18 string,
  col_1col_1col_1col_1col_1col_19 string,
  col_1col_1col_1col_1col_1col_110 string,
  col_1col_1col_1col_1col_1col_111 string,
  col_1col_1col_1col_1col_1col_112 string,
  col_1col_1col_1col_1col_1col_113 string,
  col_1col_1col_1col_1col_1col_114 string,
  col_1col_1col_1col_1col_1col_115 string,
  col_1col_1col_1col_1col_1col_116 string,
  col_1col_1col_1col_1col_1col_117 string,
  col_1col_1col_1col_1col_1col_118 string,
  col_1col_1col_1col_1col_1col_119 string,
  col_1col_1col_1col_1col_1col_120 string,
  col_1col_1col_1col_1col_1col_121 string,
  col_1col_1col_1col_1col_1col_122 string,
  col_1col_1col_1col_1col_1col_123 string,
  col_1col_1col_1col_1col_1col_124 string,
  col_1col_1col_1col_1col_1col_125 string,
  col_1col_1col_1col_1col_1col_126 string,
  col_1col_1col_1col_1col_1col_127 string,
  col_1col_1col_1col_1col_1col_128 string,
  col_1col_1col_1col_1col_1col_129 string,
  col_1col_1col_1col_1col_1col_130 string,
  col_1col_1col_1col_1col_1col_131 string,
  col_1col_1col_1col_1col_1col_132 string,
  col_1col_1col_1col_1col_1col_133 string,
  col_1col_1col_1col_1col_1col_134 string,
  col_1col_1col_1col_1col_1col_135 string,
  col_1col_1col_1col_1col_1col_136 string,
  col_1col_1col_1col_1col_1col_137 string,
  col_1col_1col_1col_1col_1col_138 string,
  col_1col_1col_1col_1col_1col_139 string,
  col_1col_1col_1col_1col_1col_140 string,
  col_1col_1col_1col_1col_1col_141 string,
  col_1col_1col_1col_1col_1col_142 string,
  col_1col_1col_1col_1col_1col_143 string,
  col_1col_1col_1col_1col_1col_144 string,
  col_1col_1col_1col_1col_1col_145 string,
  col_1col_1col_1col_1col_1col_146 string,
  col_1col_1col_1col_1col_1col_147 string,
  col_1col_1col_1col_1col_1col_148 string,
  col_1col_1col_1col_1col_1col_149 string,
  col_1col_1col_1col_1col_1col_150 string,
  col_1col_1col_1col_1col_1col_151 string,
  col_1col_1col_1col_1col_1col_152 string,
  col_1col_1col_1col_1col_1col_153 string,
  col_1col_1col_1col_1col_1col_154 string,
  col_1col_1col_1col_1col_1col_155 string,
  col_1col_1col_1col_1col_1col_156 string,
  col_1col_1col_1col_1col_1col_157 string,
  col_1col_1col_1col_1col_1col_158 string,
  col_1col_1col_1col_1col_1col_159 string,
  col_1col_1col_1col_1col_1col_160 string,
  col_1col_1col_1col_1col_1col_161 string,
  col_1col_1col_1col_1col_1col_162 string,
  col_1col_1col_1col_1col_1col_163 string,
  col_1col_1col_1col_1col_1col_164 string,
  col_1col_1col_1col_1col_1col_165 string,
  col_1col_1col_1col_1col_1col_166 string,
  col_1col_1col_1col_1col_1col_167 string,
  col_1col_1col_1col_1col_1col_168 string,
  col_1col_1col_1col_1col_1col_169 string,
  col_1col_1col_1col_1col_1col_170 string,
  col_1col_1col_1col_1col_1col_171 string,
  col_1col_1col_1col_1col_1col_172 string,
  col_1col_1col_1col_1col_1col_173 string,
  col_1col_1col_1col_1col_1col_174 string,
  col_1col_1col_1col_1col_1col_175 string,
  col_1col_1col_1col_1col_1col_176 string,
  col_1col_1col_1col_1col_1col_177 string,
  col_1col_1col_1col_1col_1col_178 string,
  col_1col_1col_1col_1col_1col_179 string,
  col_1col_1col_1col_1col_1col_180 string,
  col_1col_1col_1col_1col_1col_181 string,
  col_1col_1col_1col_1col_1col_182 string,
  col_1col_1col_1col_1col_1col_183 string,
  col_1col_1col_1col_1col_1col_184 string,
  col_1col_1col_1col_1col_1col_185 string,
  col_1col_1col_1col_1col_1col_186 string,
  col_1col_1col_1col_1col_1col_187 string,
  col_1col_1col_1col_1col_1col_188 string,
  col_1col_1col_1col_1col_1col_189 string,
  col_1col_1col_1col_1col_1col_190 string,
  col_1col_1col_1col_1col_1col_191 string,
  col_1col_1col_1col_1col_1col_192 string,
  col_1col_1col_1col_1col_1col_193 string,
  col_1col_1col_1col_1col_1col_194 string,
  col_1col_1col_1col_1col_1col_195 string,
  col_1col_1col_1col_1col_1col_196 string,
  col_1col_1col_1col_1col_1col_197 string,
  col_1col_1col_1col_1col_1col_198 string,
  col_1col_1col_1col_1col_1col_199 string,
  col_1col_1col_1col_1col_1col_1100 string,
  col_1col_1col_1col_1col_1col_1101 string,
  col_1col_1col_1col_1col_1col_1102 string,
  col_1col_1col_1col_1col_1col_1103 string,
  col_1col_1col_1col_1col_1col_1104 string,
  col_1col_1col_1col_1col_1col_1105 string,
  col_1col_1col_1col_1col_1col_1106 string,
  col_1col_1col_1col_1col_1col_1107 string,
  col_1col_1col_1col_1col_1col_1108 string,
  col_1col_1col_1col_1col_1col_1109 string,
  col_1col_1col_1col_1col_1col_1110 string,
  col_1col_1col_1col_1col_1col_1111 string,
  col_1col_1col_1col_1col_1col_1112 string,
  col_1col_1col_1col_1col_1col_1113 string,
  col_1col_1col_1col_1col_1col_1114 string,
  col_1col_1col_1col_1col_1col_1115 string,
  col_1col_1col_1col_1col_1col_1116 string,
  col_1col_1col_1col_1col_1col_1117 string,
  col_1col_1col_1col_1col_1col_1118 string,
  col_1col_1col_1col_1col_1col_1119 string,
  col_1col_1col_1col_1col_1col_1120 string,
  col_1col_1col_1col_1col_1col_1121 string,
  col_1col_1col_1col_1col_1col_1122 string,
  col_1col_1col_1col_1col_1col_1123 string,
  col_1col_1col_1col_1col_1col_1124 string,
  col_1col_1col_1col_1col_1col_1125 string,
  col_1col_1col_1col_1col_1col_1126 string,
  col_1col_1col_1col_1col_1col_1127 string,
  col_1col_1col_1col_1col_1col_1128 string,
  col_1col_1col_1col_1col_1col_1129 string,
  col_1col_1col_1col_1col_1col_1130 string,
  col_1col_1col_1col_1col_1col_1131 string,
  col_1col_1col_1col_1col_1col_1132 string,
  col_1col_1col_1col_1col_1col_1133 string,
  col_1col_1col_1col_1col_1col_1134 string,
  col_1col_1col_1col_1col_1col_1135 string,
  col_1col_1col_1col_1col_1col_1136 string,
  col_1col_1col_1col_1col_1col_1137 string,
  col_1col_1col_1col_1col_1col_1138 string,
  col_1col_1col_1col_1col_1col_1139 string,
  col_1col_1col_1col_1col_1col_1140 string,
  col_1col_1col_1col_1col_1col_1141 string,
  col_1col_1col_1col_1col_1col_1142 string,
  col_1col_1col_1col_1col_1col_1143 string,
  col_1col_1col_1col_1col_1col_1144 string,
  col_1col_1col_1col_1col_1col_1145 string,
  col_1col_1col_1col_1col_1col_1146 string,
  col_1col_1col_1col_1col_1col_1147 string,
  col_1col_1col_1col_1col_1col_1148 string,
  col_1col_1col_1col_1col_1col_1149 string,
  col_1col_1col_1col_1col_1col_1150 string,
  col_1col_1col_1col_1col_1col_1151 string,
  col_1col_1col_1col_1col_1col_1152 string,
  col_1col_1col_1col_1col_1col_1153 string,
  col_1col_1col_1col_1col_1col_1154 string,
  col_1col_1col_1col_1col_1col_1155 string,
  col_1col_1col_1col_1col_1col_1156 string,
  col_1col_1col_1col_1col_1col_1157 string,
  col_1col_1col_1col_1col_1col_1158 string)
PARTITIONED BY (
  `partition_col` string);

alter table alterPartTbl add partition(partition_col='CCL');

drop table alterPartTbl;



