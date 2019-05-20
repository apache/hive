-- this test creates a scenario where there will be a 47 view join, that will create a plan
-- using map join and backup conditional tasks.  this scenario is covered in hive-20489.

drop table if exists test_hive_1035 purge;

create table test_hive_1035
(
        test_hive_1018 string
        ,test_hive_1004 string
        ,test_hive_1025 string
        ,test_hive_23 string
        ,test_hive_27 string
        ,test_hive_29 string
        ,test_hive_30 string
        ,test_hive_97 string
        ,test_hive_96 string
        ,test_hive_98 string
        ,test_hive_101 string
        ,test_hive_102 string
        ,test_hive_109 string
        ,test_hive_111 string
        ,test_hive_112 string
        ,test_hive_113 string
        ,test_hive_114 string
        ,test_hive_115 string
        ,test_hive_78 string
        ,test_hive_79 string
        ,test_hive_24 string
        ,test_hive_26 string
        ,test_hive_110 string
        ,test_hive_77 string
        ,test_hive_87 string
        ,test_hive_92 string
        ,test_hive_90 string
        ,test_hive_74 string
        ,test_hive_85 string
        ,test_hive_81 string
        ,test_hive_82 string
        ,test_hive_106 string
        ,test_hive_107 string
        ,test_hive_108 string
        ,test_hive_75 string
        ,test_hive_86 string
        ,test_hive_76 string
        ,test_hive_89 string
        ,test_hive_88 string
        ,test_hive_91 string
        ,test_hive_71 string
        ,test_hive_72 string
        ,test_hive_73 string
        ,test_hive_80 string
        ,test_hive_103 string
        ,test_hive_104 string
        ,test_hive_1002 string
        ,test_hive_1003 string
        ,test_hive_25 string
        ,test_hive_28 string
        ,test_hive_93 string
        ,test_hive_94 string
        ,test_hive_95 string
        ,test_hive_99 string
        ,test_hive_105 string
        ,test_hive_83 string
        ,test_hive_84 string
        ,test_hive_100 string
        ,test_hive_1023 string
        ,test_hive_1024 string
        ,test_hive_1010 string
        ,test_hive_1010_a_d string
        ,test_hive_1010_a_g string
        ,test_hive_1026 string
        ,test_hive_1000 string
        ,test_hive_1001 string
        ,test_hive_1030 string
        ,test_hive_1030_1 string
        ,test_hive_1030_2 string
        ,test_hive_1030_3 string
        ,test_hive_1021 string
        ,test_hive_1020 string
        ,test_hive_1022 string
        ,test_hive_1019 string
        ,test_hive_1027 string
        ,test_hive_1028 string
        ,test_hive_1029 string
        ,test_hive_1005 string
        ,test_hive_1005_a_d string
        ,test_hive_1005_psr string
        ,test_hive_1005_psr_a_d string
        ,test_hive_1005_psr_e string
        ,test_hive_1013 string
        ,test_hive_1013_a_d string
        ,test_hive_1013_psr string
        ,test_hive_1013_psr_a_d string
        ,test_hive_1013_psr_e string
        ,test_hive_1034 string
)
partitioned by (ds int, ts int)
stored as parquet;

create table if not exists test_hive_1038
(
        test_hive_1018 string
        ,test_hive_1004 string
        ,test_hive_1025 string
        ,test_hive_23 string
        ,test_hive_27 string
        ,test_hive_29 string
        ,test_hive_30 string
        ,test_hive_97 string
        ,test_hive_96 string
        ,test_hive_98 string
        ,test_hive_101 string
        ,test_hive_102 string
        ,test_hive_109 string
        ,test_hive_111 string
        ,test_hive_112 string
        ,test_hive_113 string
        ,test_hive_114 string
        ,test_hive_115 string
        ,test_hive_78 string
        ,test_hive_79 string
        ,test_hive_24 string
        ,test_hive_26 string
        ,test_hive_110 string
        ,test_hive_77 string
        ,test_hive_87 string
        ,test_hive_92 string
        ,test_hive_90 string
        ,test_hive_74 string
        ,test_hive_85 string
        ,test_hive_81 string
        ,test_hive_82 string
        ,test_hive_106 string
        ,test_hive_107 string
        ,test_hive_108 string
        ,test_hive_75 string
        ,test_hive_86 string
        ,test_hive_76 string
        ,test_hive_89 string
        ,test_hive_88 string
        ,test_hive_91 string
        ,test_hive_71 string
        ,test_hive_72 string
        ,test_hive_73 string
        ,test_hive_80 string
        ,test_hive_103 string
        ,test_hive_104 string
        ,test_hive_1002 string
        ,test_hive_1003 string
        ,test_hive_25 string
        ,test_hive_28 string
        ,test_hive_93 string
        ,test_hive_94 string
        ,test_hive_95 string
        ,test_hive_99 string
        ,test_hive_105 string
        ,test_hive_83 string
        ,test_hive_84 string
        ,test_hive_100 string
        ,test_hive_1023 string
        ,test_hive_1024 string
        ,test_hive_1010 string
        ,test_hive_1010_a_d string
        ,test_hive_1010_a_g string
        ,test_hive_1026 string
        ,test_hive_1000 string
        ,test_hive_1001 string
        ,test_hive_1030 string
        ,test_hive_1030_1 string
        ,test_hive_1030_2 string
        ,test_hive_1030_3 string
        ,test_hive_1021 string
        ,test_hive_1020 string
        ,test_hive_1022 string
        ,test_hive_1019 string
        ,test_hive_1027 string
        ,test_hive_1028 string
        ,test_hive_1029 string
        ,test_hive_1005 string
        ,test_hive_1005_a_d string
        ,test_hive_1005_psr string
        ,test_hive_1005_psr_a_d string
        ,test_hive_1005_psr_e string
        ,test_hive_1013 string
        ,test_hive_1013_a_d string
        ,test_hive_1013_psr string
        ,test_hive_1013_psr_a_d string
        ,test_hive_1013_psr_e string
        ,test_hive_1034 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1037 purge;

create table if not exists test_hive_1037
(
max_partition bigint
);

drop view if exists test_hive_1040;

create view if not exists test_hive_1040
as
select
        cast(test_hive_1018 as int) as test_hive_1018
        ,cast(test_hive_1004 as int) as test_hive_1004
        ,cast(test_hive_1025 as int) as test_hive_1025
        ,cast(test_hive_23 as string) as test_hive_23
        ,cast(test_hive_27 as string) as test_hive_27
        ,cast(test_hive_29 as string) as test_hive_29
        ,cast(test_hive_30 as string) as test_hive_30
        ,cast(test_hive_97 as string) as test_hive_97
        ,cast(test_hive_96 as string) as test_hive_96
        ,cast(test_hive_98 as string) as test_hive_98
        ,cast(test_hive_101 as string) as test_hive_101
        ,cast(test_hive_102 as string) as test_hive_102
        ,cast(test_hive_109 as string) as test_hive_109
        ,cast(test_hive_111 as string) as test_hive_111
        ,cast(test_hive_112 as string) as test_hive_112
        ,cast(test_hive_113 as string) as test_hive_113
        ,cast(test_hive_114 as string) as test_hive_114
        ,cast(test_hive_115 as string) as test_hive_115
        ,cast(test_hive_78 as string) as test_hive_78
        ,cast(test_hive_79 as string) as test_hive_79
        ,cast(test_hive_24 as string) as test_hive_24
        ,cast(test_hive_26 as string) as test_hive_26
        ,cast(test_hive_110 as string) as test_hive_110
        ,cast(test_hive_77 as string) as test_hive_77
        ,cast(test_hive_87 as string) as test_hive_87
        ,cast(test_hive_92 as string) as test_hive_92
        ,cast(test_hive_90 as string) as test_hive_90
        ,cast(test_hive_74 as string) as test_hive_74
        ,cast(test_hive_85 as string) as test_hive_85
        ,cast(test_hive_81 as string) as test_hive_81
        ,cast(test_hive_82 as string) as test_hive_82
        ,cast(test_hive_106 as string) as test_hive_106
        ,cast(test_hive_107 as string) as test_hive_107
        ,cast(test_hive_108 as string) as test_hive_108
        ,cast(test_hive_75 as string) as test_hive_75
        ,cast(test_hive_86 as string) as test_hive_86
        ,cast(test_hive_76 as string) as test_hive_76
        ,cast(test_hive_89 as string) as test_hive_89
        ,cast(test_hive_88 as string) as test_hive_88
        ,cast(test_hive_91 as string) as test_hive_91
        ,cast(test_hive_71 as string) as test_hive_71
        ,cast(test_hive_72 as string) as test_hive_72
        ,cast(test_hive_73 as string) as test_hive_73
        ,cast(test_hive_80 as string) as test_hive_80
        ,cast(test_hive_103 as string) as test_hive_103
        ,cast(test_hive_104 as string) as test_hive_104
        ,cast(test_hive_1002 as string) as test_hive_1002
        ,cast(test_hive_1003 as string) as test_hive_1003
        ,cast(from_unixtime(unix_timestamp(test_hive_25,'yyyymmdd'), 'yyyy-mm-dd') as timestamp) as test_hive_25
        ,cast(test_hive_28 as string) as test_hive_28
        ,cast(test_hive_93 as string) as test_hive_93
        ,cast(test_hive_94 as string) as test_hive_94
        ,cast(test_hive_95 as string) as test_hive_95
        ,cast(test_hive_99 as string) as test_hive_99
        ,cast(test_hive_105 as string) as test_hive_105
        ,cast(test_hive_83 as string) as test_hive_83
        ,cast(test_hive_84 as string) as test_hive_84
        ,cast(test_hive_100 as string) as test_hive_100
        ,cast(test_hive_1023 as int) as test_hive_1023
        ,cast(test_hive_1024 as int) as test_hive_1024
        ,cast(test_hive_1010 as int) as test_hive_1010
        ,cast(test_hive_1010_a_d as int) as test_hive_1010_a_d
        ,cast(test_hive_1010_a_g as int) as test_hive_1010_a_g
        ,cast(test_hive_1026 as double) as test_hive_1026
        ,cast(test_hive_1000 as double) as test_hive_1000
        ,cast(test_hive_1001 as double) as test_hive_1001
        ,cast(test_hive_1030 as int) as test_hive_1030
        ,cast(test_hive_1030_1 as int) as test_hive_1030_1
        ,cast(test_hive_1030_2 as int) as test_hive_1030_2
        ,cast(test_hive_1030_3 as int) as test_hive_1030_3
        ,cast(test_hive_1021 as double) as test_hive_1021
        ,cast(test_hive_1020 as double) as test_hive_1020
        ,cast(test_hive_1022 as int) as test_hive_1022
        ,cast(test_hive_1019 as int) as test_hive_1019
        ,cast(test_hive_1027 as double) as test_hive_1027
        ,cast(test_hive_1028 as double) as test_hive_1028
        ,cast(test_hive_1029 as double) as test_hive_1029
        ,cast(test_hive_1005 as int) as test_hive_1005
        ,cast(test_hive_1005_a_d as int) as test_hive_1005_a_d
        ,cast(test_hive_1005_psr as int) as test_hive_1005_psr
        ,cast(test_hive_1005_psr_a_d as int) as test_hive_1005_psr_a_d
        ,cast(test_hive_1005_psr_e as int) as test_hive_1005_psr_e
        ,cast(test_hive_1013 as int) as test_hive_1013
        ,cast(test_hive_1013_a_d as int) as test_hive_1013_a_d
        ,cast(test_hive_1013_psr as int) as test_hive_1013_psr
        ,cast(test_hive_1013_psr_a_d as int) as test_hive_1013_psr_a_d
        ,cast(test_hive_1013_psr_e as int) as test_hive_1013_psr_e
        ,cast(from_unixtime(unix_timestamp(test_hive_1034,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1034
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1038
;

drop view if exists test_hive_1039;

create view test_hive_1039
as
select
        test_hive_1018 as test_hive_1018
        ,test_hive_1004 as test_hive_1004
        ,test_hive_1025 as test_hive_1025
        ,test_hive_23 as test_hive_23
        ,test_hive_27 as test_hive_27
        ,test_hive_29 as test_hive_29
        ,test_hive_30 as test_hive_30
        ,test_hive_97 as test_hive_97
        ,test_hive_96 as test_hive_96
        ,test_hive_98 as test_hive_98
        ,test_hive_101 as test_hive_101
        ,test_hive_102 as test_hive_102
        ,test_hive_109 as test_hive_109
        ,test_hive_111 as test_hive_111
        ,test_hive_112 as test_hive_112
        ,test_hive_113 as test_hive_113
        ,test_hive_114 as test_hive_114
        ,test_hive_115 as test_hive_115
        ,test_hive_78 as test_hive_78
        ,test_hive_79 as test_hive_79
        ,test_hive_24 as test_hive_24
        ,test_hive_26 as test_hive_26
        ,test_hive_110 as test_hive_110
        ,test_hive_77 as test_hive_77
        ,test_hive_87 as test_hive_87
        ,test_hive_92 as test_hive_92
        ,test_hive_90 as test_hive_90
        ,test_hive_74 as test_hive_74
        ,test_hive_85 as test_hive_85
        ,test_hive_81 as test_hive_81
        ,test_hive_82 as test_hive_82
        ,test_hive_106 as test_hive_106
        ,test_hive_107 as test_hive_107
        ,test_hive_108 as test_hive_108
        ,test_hive_75 as test_hive_75
        ,test_hive_86 as test_hive_86
        ,test_hive_76 as test_hive_76
        ,test_hive_89 as test_hive_89
        ,test_hive_88 as test_hive_88
        ,test_hive_91 as test_hive_91
        ,test_hive_71 as test_hive_71
        ,test_hive_72 as test_hive_72
        ,test_hive_73 as test_hive_73
        ,test_hive_80 as test_hive_80
        ,test_hive_103 as test_hive_103
        ,test_hive_104 as test_hive_104
        ,test_hive_1002 as test_hive_1002
        ,test_hive_1003 as test_hive_1003
        ,test_hive_25 as test_hive_25
        ,test_hive_28 as test_hive_28
        ,test_hive_93 as test_hive_93
        ,test_hive_94 as test_hive_94
        ,test_hive_95 as test_hive_95
        ,test_hive_99 as test_hive_99
        ,test_hive_105 as test_hive_105
        ,test_hive_83 as test_hive_83
        ,test_hive_84 as test_hive_84
        ,test_hive_100 as test_hive_100
        ,test_hive_1023 as test_hive_1023
        ,test_hive_1024 as test_hive_1024
        ,test_hive_1010 as test_hive_1010
        ,test_hive_1010_a_d as test_hive_1010_a_d
        ,test_hive_1010_a_g as test_hive_1010_a_g
        ,test_hive_1026 as test_hive_1026
        ,test_hive_1000 as test_hive_1000
        ,test_hive_1001 as test_hive_1001
        ,test_hive_1030 as test_hive_1030
        ,test_hive_1030_1 as test_hive_1030_1
        ,test_hive_1030_2 as test_hive_1030_2
        ,test_hive_1030_3 as test_hive_1030_3
        ,test_hive_1021 as test_hive_1021
        ,test_hive_1020 as test_hive_1020
        ,test_hive_1022 as test_hive_1022
        ,test_hive_1019 as test_hive_1019
        ,test_hive_1027 as test_hive_1027
        ,test_hive_1028 as test_hive_1028
        ,test_hive_1029 as test_hive_1029
        ,test_hive_1005 as test_hive_1005
        ,test_hive_1005_a_d as test_hive_1005_a_d
        ,test_hive_1005_psr as test_hive_1005_psr
        ,test_hive_1005_psr_a_d as test_hive_1005_psr_a_d
        ,test_hive_1005_psr_e as test_hive_1005_psr_e
        ,test_hive_1013 as test_hive_1013
        ,test_hive_1013_a_d as test_hive_1013_a_d
        ,test_hive_1013_psr as test_hive_1013_psr
        ,test_hive_1013_psr_a_d as test_hive_1013_psr_a_d
        ,test_hive_1013_psr_e as test_hive_1013_psr_e
        ,test_hive_1034 as test_hive_1034
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
        ,ds
from test_hive_1040 t1
;

drop view if exists test_hive_1036;

create view test_hive_1036
as
select t1.*
from test_hive_1039 t1
inner join test_hive_1037 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1054 purge;

create table test_hive_1054
(
        test_hive_1047 string
        ,test_hive_1045 string
        ,test_hive_1048 string
        ,test_hive_132 string
        ,test_hive_146 string
        ,test_hive_1043 string
        ,test_hive_149 string
        ,test_hive_150 string
        ,test_hive_119 string
        ,test_hive_118 string
        ,test_hive_120 string
        ,test_hive_151 string
        ,test_hive_116 string
        ,test_hive_117 string
        ,test_hive_121 string
        ,test_hive_122 string
        ,test_hive_152 string
        ,test_hive_155 string
        ,test_hive_159 string
        ,test_hive_131 string
        ,test_hive_140 string
        ,test_hive_145 string
        ,test_hive_143 string
        ,test_hive_128 string
        ,test_hive_138 string
        ,test_hive_134 string
        ,test_hive_135 string
        ,test_hive_156 string
        ,test_hive_157 string
        ,test_hive_158 string
        ,test_hive_129 string
        ,test_hive_139 string
        ,test_hive_130 string
        ,test_hive_142 string
        ,test_hive_141 string
        ,test_hive_144 string
        ,test_hive_125 string
        ,test_hive_126 string
        ,test_hive_127 string
        ,test_hive_133 string
        ,test_hive_154 string
        ,test_hive_123 string
        ,test_hive_160 string
        ,test_hive_136 string
        ,test_hive_137 string
        ,test_hive_124 string
        ,test_hive_153 string
        ,test_hive_148 string
        ,test_hive_147 string
        ,test_hive_1052 string
        ,test_hive_1051 string
        ,test_hive_1041 string
        ,test_hive_1042 string
        ,test_hive_1044 string
        ,test_hive_1046 string
        ,test_hive_1050 string
        ,test_hive_1049 string
        ,test_hive_1053 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1057
(
        test_hive_1047 string
        ,test_hive_1045 string
        ,test_hive_1048 string
        ,test_hive_132 string
        ,test_hive_146 string
        ,test_hive_1043 string
        ,test_hive_149 string
        ,test_hive_150 string
        ,test_hive_119 string
        ,test_hive_118 string
        ,test_hive_120 string
        ,test_hive_151 string
        ,test_hive_116 string
        ,test_hive_117 string
        ,test_hive_121 string
        ,test_hive_122 string
        ,test_hive_152 string
        ,test_hive_155 string
        ,test_hive_159 string
        ,test_hive_131 string
        ,test_hive_140 string
        ,test_hive_145 string
        ,test_hive_143 string
        ,test_hive_128 string
        ,test_hive_138 string
        ,test_hive_134 string
        ,test_hive_135 string
        ,test_hive_156 string
        ,test_hive_157 string
        ,test_hive_158 string
        ,test_hive_129 string
        ,test_hive_139 string
        ,test_hive_130 string
        ,test_hive_142 string
        ,test_hive_141 string
        ,test_hive_144 string
        ,test_hive_125 string
        ,test_hive_126 string
        ,test_hive_127 string
        ,test_hive_133 string
        ,test_hive_154 string
        ,test_hive_123 string
        ,test_hive_160 string
        ,test_hive_136 string
        ,test_hive_137 string
        ,test_hive_124 string
        ,test_hive_153 string
        ,test_hive_148 string
        ,test_hive_147 string
        ,test_hive_1052 string
        ,test_hive_1051 string
        ,test_hive_1041 string
        ,test_hive_1042 string
        ,test_hive_1044 string
        ,test_hive_1046 string
        ,test_hive_1050 string
        ,test_hive_1049 string
        ,test_hive_1053 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1056 purge;

create table if not exists test_hive_1056
(
max_partition bigint
);

drop view if exists test_hive_1059;

create view if not exists test_hive_1059
as
select
        cast(test_hive_1047 as int) as test_hive_1047
        ,cast(test_hive_1045 as int) as test_hive_1045
        ,cast(test_hive_1048 as int) as test_hive_1048
        ,cast(test_hive_132 as string) as test_hive_132
        ,cast(test_hive_146 as string) as test_hive_146
        ,cast(test_hive_1043 as string) as test_hive_1043
        ,cast(test_hive_149 as string) as test_hive_149
        ,cast(test_hive_150 as string) as test_hive_150
        ,cast(from_unixtime(unix_timestamp(test_hive_119,'yyyymmdd'), 'yyyy-mm-dd') as timestamp) as test_hive_119
        ,cast(test_hive_118 as string) as test_hive_118
        ,cast(test_hive_120 as string) as test_hive_120
        ,cast(test_hive_151 as string) as test_hive_151
        ,cast(test_hive_116 as string) as test_hive_116
        ,cast(test_hive_117 as string) as test_hive_117
        ,cast(test_hive_121 as string) as test_hive_121
        ,cast(test_hive_122 as string) as test_hive_122
        ,cast(test_hive_152 as string) as test_hive_152
        ,cast(test_hive_155 as string) as test_hive_155
        ,cast(test_hive_159 as string) as test_hive_159
        ,cast(test_hive_131 as string) as test_hive_131
        ,cast(test_hive_140 as string) as test_hive_140
        ,cast(test_hive_145 as string) as test_hive_145
        ,cast(test_hive_143 as string) as test_hive_143
        ,cast(test_hive_128 as string) as test_hive_128
        ,cast(test_hive_138 as string) as test_hive_138
        ,cast(test_hive_134 as string) as test_hive_134
        ,cast(test_hive_135 as string) as test_hive_135
        ,cast(test_hive_156 as string) as test_hive_156
        ,cast(test_hive_157 as string) as test_hive_157
        ,cast(test_hive_158 as string) as test_hive_158
        ,cast(test_hive_129 as string) as test_hive_129
        ,cast(test_hive_139 as string) as test_hive_139
        ,cast(test_hive_130 as string) as test_hive_130
        ,cast(test_hive_142 as string) as test_hive_142
        ,cast(test_hive_141 as string) as test_hive_141
        ,cast(test_hive_144 as string) as test_hive_144
        ,cast(test_hive_125 as string) as test_hive_125
        ,cast(test_hive_126 as string) as test_hive_126
        ,cast(test_hive_127 as string) as test_hive_127
        ,cast(test_hive_133 as string) as test_hive_133
        ,cast(test_hive_154 as string) as test_hive_154
        ,cast(test_hive_123 as string) as test_hive_123
        ,cast(test_hive_160 as string) as test_hive_160
        ,cast(test_hive_136 as string) as test_hive_136
        ,cast(test_hive_137 as string) as test_hive_137
        ,cast(test_hive_124 as string) as test_hive_124
        ,cast(test_hive_153 as string) as test_hive_153
        ,cast(test_hive_148 as string) as test_hive_148
        ,cast(test_hive_147 as string) as test_hive_147
        ,cast(test_hive_1052 as int) as test_hive_1052
        ,cast(test_hive_1051 as int) as test_hive_1051
        ,cast(test_hive_1041 as int) as test_hive_1041
        ,cast(test_hive_1042 as int) as test_hive_1042
        ,cast(test_hive_1044 as int) as test_hive_1044
        ,cast(test_hive_1046 as int) as test_hive_1046
        ,cast(test_hive_1050 as int) as test_hive_1050
        ,cast(test_hive_1049 as int) as test_hive_1049
        ,cast(from_unixtime(unix_timestamp(test_hive_1053,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1053
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1057
;

drop view if exists test_hive_1058;

create view test_hive_1058
as
select
        test_hive_1047 as test_hive_1047
        ,test_hive_1045 as test_hive_1045
        ,test_hive_1048 as test_hive_1048
        ,test_hive_132 as test_hive_132
        ,test_hive_146 as test_hive_146
        ,test_hive_1043 as test_hive_1043
        ,test_hive_149 as test_hive_149
        ,test_hive_150 as test_hive_150
        ,test_hive_119 as test_hive_119
        ,test_hive_118 as test_hive_118
        ,test_hive_120 as test_hive_120
        ,test_hive_151 as test_hive_151
        ,test_hive_116 as test_hive_116
        ,test_hive_117 as test_hive_117
        ,test_hive_121 as test_hive_121
        ,test_hive_122 as test_hive_122
        ,test_hive_152 as test_hive_152
        ,test_hive_155 as test_hive_155
        ,test_hive_159 as test_hive_159
        ,test_hive_131 as test_hive_131
        ,test_hive_140 as test_hive_140
        ,test_hive_145 as test_hive_145
        ,test_hive_143 as test_hive_143
        ,test_hive_128 as test_hive_128
        ,test_hive_138 as test_hive_138
        ,test_hive_134 as test_hive_134
        ,test_hive_135 as test_hive_135
        ,test_hive_156 as test_hive_156
        ,test_hive_157 as test_hive_157
        ,test_hive_158 as test_hive_158
        ,test_hive_129 as test_hive_129
        ,test_hive_139 as test_hive_139
        ,test_hive_130 as test_hive_130
        ,test_hive_142 as test_hive_142
        ,test_hive_141 as test_hive_141
        ,test_hive_144 as test_hive_144
        ,test_hive_125 as test_hive_125
        ,test_hive_126 as test_hive_126
        ,test_hive_127 as test_hive_127
        ,test_hive_133 as test_hive_133
        ,test_hive_154 as test_hive_154
        ,test_hive_123 as test_hive_123
        ,test_hive_160 as test_hive_160
        ,test_hive_136 as test_hive_136
        ,test_hive_137 as test_hive_137
        ,test_hive_124 as test_hive_124
        ,test_hive_153 as test_hive_153
        ,test_hive_148 as test_hive_148
        ,test_hive_147 as test_hive_147
        ,test_hive_1052 as test_hive_1052
        ,test_hive_1051 as test_hive_1051
        ,test_hive_1041 as test_hive_1041
        ,test_hive_1042 as test_hive_1042
        ,test_hive_1044 as test_hive_1044
        ,test_hive_1046 as test_hive_1046
        ,test_hive_1050 as test_hive_1050
        ,test_hive_1049 as test_hive_1049
        ,test_hive_1053 as test_hive_1053
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
        ,ds
from test_hive_1059 t1
;

drop view if exists test_hive_1055;

create view test_hive_1055
as
select t1.*
from test_hive_1058 t1
inner join test_hive_1056 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1083 purge;

create table test_hive_1083
(
        test_hive_1072 string
        ,test_hive_1065 string
        ,test_hive_1073 string
        ,test_hive_161 string
        ,test_hive_162 string
        ,test_hive_163 string
        ,test_hive_164 string
        ,test_hive_167 string
        ,test_hive_168 string
        ,test_hive_170 string
        ,test_hive_197 string
        ,test_hive_198 string
        ,test_hive_200 string
        ,test_hive_201 string
        ,test_hive_202 string
        ,test_hive_203 string
        ,test_hive_205 string
        ,test_hive_206 string
        ,test_hive_212 string
        ,test_hive_213 string
        ,test_hive_178 string
        ,test_hive_1060 string
        ,test_hive_1061 string
        ,test_hive_10612 string
        ,test_hive_1063 string
        ,test_hive_1064 string
        ,test_hive_165 string
        ,test_hive_166 string
        ,test_hive_169 string
        ,test_hive_193 string
        ,test_hive_194 string
        ,test_hive_195 string
        ,test_hive_196 string
        ,test_hive_204 string
        ,test_hive_207 string
        ,test_hive_208 string
        ,test_hive_209 string
        ,test_hive_210 string
        ,test_hive_211 string
        ,test_hive_171 string
        ,test_hive_172 string
        ,test_hive_173 string
        ,test_hive_174 string
        ,test_hive_175 string
        ,test_hive_176 string
        ,test_hive_177 string
        ,test_hive_179 string
        ,test_hive_180 string
        ,test_hive_181 string
        ,test_hive_182 string
        ,test_hive_183 string
        ,test_hive_184 string
        ,test_hive_185 string
        ,test_hive_186 string
        ,test_hive_187 string
        ,test_hive_188 string
        ,test_hive_189 string
        ,test_hive_190 string
        ,test_hive_191 string
        ,test_hive_192 string
        ,test_hive_1067 string
        ,test_hive_1067_a_g string
        ,test_hive_1067_h string
        ,test_hive_1066 string
        ,test_hive_1070 string
        ,test_hive_1070_a_d string
        ,test_hive_1074 string
        ,test_hive_1074_bp string
        ,test_hive_1074_cont string
        ,test_hive_1074_lag string
        ,test_hive_1078 string
        ,test_hive_1078_bp string
        ,test_hive_1078_cont string
        ,test_hive_1078_lag string
        ,test_hive_199 string
        ,test_hive_1082 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1086
(
        test_hive_1072 string
        ,test_hive_1065 string
        ,test_hive_1073 string
        ,test_hive_161 string
        ,test_hive_162 string
        ,test_hive_163 string
        ,test_hive_164 string
        ,test_hive_167 string
        ,test_hive_168 string
        ,test_hive_170 string
        ,test_hive_197 string
        ,test_hive_198 string
        ,test_hive_200 string
        ,test_hive_201 string
        ,test_hive_202 string
        ,test_hive_203 string
        ,test_hive_205 string
        ,test_hive_206 string
        ,test_hive_212 string
        ,test_hive_213 string
        ,test_hive_178 string
        ,test_hive_1060 string
        ,test_hive_1061 string
        ,test_hive_10612 string
        ,test_hive_1063 string
        ,test_hive_1064 string
        ,test_hive_165 string
        ,test_hive_166 string
        ,test_hive_169 string
        ,test_hive_193 string
        ,test_hive_194 string
        ,test_hive_195 string
        ,test_hive_196 string
        ,test_hive_204 string
        ,test_hive_207 string
        ,test_hive_208 string
        ,test_hive_209 string
        ,test_hive_210 string
        ,test_hive_211 string
        ,test_hive_171 string
        ,test_hive_172 string
        ,test_hive_173 string
        ,test_hive_174 string
        ,test_hive_175 string
        ,test_hive_176 string
        ,test_hive_177 string
        ,test_hive_179 string
        ,test_hive_180 string
        ,test_hive_181 string
        ,test_hive_182 string
        ,test_hive_183 string
        ,test_hive_184 string
        ,test_hive_185 string
        ,test_hive_186 string
        ,test_hive_187 string
        ,test_hive_188 string
        ,test_hive_189 string
        ,test_hive_190 string
        ,test_hive_191 string
        ,test_hive_192 string
        ,test_hive_1067 string
        ,test_hive_1067_a_g string
        ,test_hive_1067_h string
        ,test_hive_1066 string
        ,test_hive_1070 string
        ,test_hive_1070_a_d string
        ,test_hive_1074 string
        ,test_hive_1074_bp string
        ,test_hive_1074_cont string
        ,test_hive_1074_lag string
        ,test_hive_1078 string
        ,test_hive_1078_bp string
        ,test_hive_1078_cont string
        ,test_hive_1078_lag string
        ,test_hive_199 string
        ,test_hive_1082 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1085 purge;

create table if not exists test_hive_1085
(
max_partition bigint
);

drop view if exists test_hive_1088;

create view if not exists test_hive_1088
as
select
        cast(test_hive_1072 as int) as test_hive_1072
        ,cast(test_hive_1065 as int) as test_hive_1065
        ,cast(test_hive_1073 as int) as test_hive_1073
        ,cast(test_hive_161 as string) as test_hive_161
        ,cast(test_hive_162 as string) as test_hive_162
        ,cast(test_hive_163 as string) as test_hive_163
        ,cast(test_hive_164 as string) as test_hive_164
        ,cast(test_hive_167 as string) as test_hive_167
        ,cast(test_hive_168 as string) as test_hive_168
        ,cast(test_hive_170 as string) as test_hive_170
        ,cast(test_hive_197 as string) as test_hive_197
        ,cast(test_hive_198 as string) as test_hive_198
        ,cast(test_hive_200 as string) as test_hive_200
        ,cast(test_hive_201 as string) as test_hive_201
        ,cast(test_hive_202 as string) as test_hive_202
        ,cast(test_hive_203 as string) as test_hive_203
        ,cast(test_hive_205 as string) as test_hive_205
        ,cast(test_hive_206 as string) as test_hive_206
        ,cast(test_hive_212 as string) as test_hive_212
        ,cast(test_hive_213 as string) as test_hive_213
        ,cast(test_hive_178 as string) as test_hive_178
        ,cast(from_unixtime(unix_timestamp(test_hive_1060,'yyyymmdd'), 'yyyy-mm-dd') as timestamp) as test_hive_1060
        ,cast(test_hive_1061 as string) as test_hive_1061
        ,cast(test_hive_10612 as string) as test_hive_10612
        ,cast(test_hive_1063 as string) as test_hive_1063
        ,cast(test_hive_1064 as string) as test_hive_1064
        ,cast(test_hive_165 as string) as test_hive_165
        ,cast(test_hive_166 as string) as test_hive_166
        ,cast(test_hive_169 as string) as test_hive_169
        ,cast(test_hive_193 as string) as test_hive_193
        ,cast(test_hive_194 as string) as test_hive_194
        ,cast(test_hive_195 as string) as test_hive_195
        ,cast(test_hive_196 as string) as test_hive_196
        ,cast(test_hive_204 as string) as test_hive_204
        ,cast(test_hive_207 as string) as test_hive_207
        ,cast(test_hive_208 as string) as test_hive_208
        ,cast(test_hive_209 as string) as test_hive_209
        ,cast(test_hive_210 as string) as test_hive_210
        ,cast(test_hive_211 as string) as test_hive_211
        ,cast(test_hive_171 as string) as test_hive_171
        ,cast(test_hive_172 as string) as test_hive_172
        ,cast(test_hive_173 as string) as test_hive_173
        ,cast(test_hive_174 as string) as test_hive_174
        ,cast(test_hive_175 as string) as test_hive_175
        ,cast(test_hive_176 as string) as test_hive_176
        ,cast(test_hive_177 as string) as test_hive_177
        ,cast(test_hive_179 as string) as test_hive_179
        ,cast(test_hive_180 as string) as test_hive_180
        ,cast(test_hive_181 as string) as test_hive_181
        ,cast(test_hive_182 as string) as test_hive_182
        ,cast(test_hive_183 as string) as test_hive_183
        ,cast(test_hive_184 as string) as test_hive_184
        ,cast(test_hive_185 as string) as test_hive_185
        ,cast(test_hive_186 as string) as test_hive_186
        ,cast(test_hive_187 as string) as test_hive_187
        ,cast(test_hive_188 as string) as test_hive_188
        ,cast(test_hive_189 as string) as test_hive_189
        ,cast(test_hive_190 as string) as test_hive_190
        ,cast(test_hive_191 as string) as test_hive_191
        ,cast(test_hive_192 as string) as test_hive_192
        ,cast(test_hive_1067 as int) as test_hive_1067
        ,cast(test_hive_1067_a_g as int) as test_hive_1067_a_g
        ,cast(test_hive_1067_h as int) as test_hive_1067_h
        ,cast(test_hive_1066 as int) as test_hive_1066
        ,cast(test_hive_1070 as int) as test_hive_1070
        ,cast(test_hive_1070_a_d as int) as test_hive_1070_a_d
        ,cast(test_hive_1074 as int) as test_hive_1074
        ,cast(test_hive_1074_bp as int) as test_hive_1074_bp
        ,cast(test_hive_1074_cont as int) as test_hive_1074_cont
        ,cast(test_hive_1074_lag as int) as test_hive_1074_lag
        ,cast(test_hive_1078 as int) as test_hive_1078
        ,cast(test_hive_1078_bp as int) as test_hive_1078_bp
        ,cast(test_hive_1078_cont as int) as test_hive_1078_cont
        ,cast(test_hive_1078_lag as int) as test_hive_1078_lag
        ,cast(test_hive_199 as string) as test_hive_199
        ,cast(from_unixtime(unix_timestamp(test_hive_1082,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1082
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1086
;

drop view if exists test_hive_1087;

create view test_hive_1087
as
select
        test_hive_1072 as test_hive_1072
        ,test_hive_1065 as test_hive_1065
        ,test_hive_1073 as test_hive_1073
        ,test_hive_161 as test_hive_161
        ,test_hive_162 as test_hive_162
        ,test_hive_163 as test_hive_163
        ,test_hive_164 as test_hive_164
        ,test_hive_167 as test_hive_167
        ,test_hive_168 as test_hive_168
        ,test_hive_170 as test_hive_170
        ,test_hive_197 as test_hive_197
        ,test_hive_198 as test_hive_198
        ,test_hive_200 as test_hive_200
        ,test_hive_201 as test_hive_201
        ,test_hive_202 as test_hive_202
        ,test_hive_203 as test_hive_203
        ,test_hive_205 as test_hive_205
        ,test_hive_206 as test_hive_206
        ,test_hive_212 as test_hive_212
        ,test_hive_213 as test_hive_213
        ,test_hive_178 as test_hive_178
        ,test_hive_1060 as test_hive_1060
        ,test_hive_1061 as test_hive_1061
        ,test_hive_10612 as test_hive_10612
        ,test_hive_1063 as test_hive_1063
        ,test_hive_1064 as test_hive_1064
        ,test_hive_165 as test_hive_165
        ,test_hive_166 as test_hive_166
        ,test_hive_169 as test_hive_169
        ,test_hive_193 as test_hive_193
        ,test_hive_194 as test_hive_194
        ,test_hive_195 as test_hive_195
        ,test_hive_196 as test_hive_196
        ,test_hive_204 as test_hive_204
        ,test_hive_207 as test_hive_207
        ,test_hive_208 as test_hive_208
        ,test_hive_209 as test_hive_209
        ,test_hive_210 as test_hive_210
        ,test_hive_211 as test_hive_211
        ,test_hive_171 as test_hive_171
        ,test_hive_172 as test_hive_172
        ,test_hive_173 as test_hive_173
        ,test_hive_174 as test_hive_174
        ,test_hive_175 as test_hive_175
        ,test_hive_176 as test_hive_176
        ,test_hive_177 as test_hive_177
        ,test_hive_179 as test_hive_179
        ,test_hive_180 as test_hive_180
        ,test_hive_181 as test_hive_181
        ,test_hive_182 as test_hive_182
        ,test_hive_183 as test_hive_183
        ,test_hive_184 as test_hive_184
        ,test_hive_185 as test_hive_185
        ,test_hive_186 as test_hive_186
        ,test_hive_187 as test_hive_187
        ,test_hive_188 as test_hive_188
        ,test_hive_189 as test_hive_189
        ,test_hive_190 as test_hive_190
        ,test_hive_191 as test_hive_191
        ,test_hive_192 as test_hive_192
        ,test_hive_1067 as test_hive_1067
        ,test_hive_1067_a_g as test_hive_1067_a_g
        ,test_hive_1067_h as test_hive_1067_h
        ,test_hive_1066 as test_hive_1066
        ,test_hive_1070 as test_hive_1070
        ,test_hive_1070_a_d as test_hive_1070_a_d
        ,test_hive_1074 as test_hive_1074
        ,test_hive_1074_bp as test_hive_1074_bp
        ,test_hive_1074_cont as test_hive_1074_cont
        ,test_hive_1074_lag as test_hive_1074_lag
        ,test_hive_1078 as test_hive_1078
        ,test_hive_1078_bp as test_hive_1078_bp
        ,test_hive_1078_cont as test_hive_1078_cont
        ,test_hive_1078_lag as test_hive_1078_lag
        ,test_hive_199 as test_hive_199
        ,test_hive_1082 as test_hive_1082
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
        ,ds
from test_hive_1088 t1
;

drop view if exists test_hive_1084;

create view test_hive_1084
as
select t1.*
from test_hive_1087 t1
inner join test_hive_1085 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1100 purge;

create table test_hive_1100
(
        test_hive_1097 string
        ,test_hive_1095 string
        ,test_hive_1098 string
        ,test_hive_1089 string
        ,test_hive_1090 string
        ,test_hive_10902 string
        ,test_hive_1092 string
        ,test_hive_1093 string
        ,test_hive_244 string
        ,test_hive_225 string
        ,test_hive_214 string
        ,test_hive_215 string
        ,test_hive_216 string
        ,test_hive_217 string
        ,test_hive_240 string
        ,test_hive_241 string
        ,test_hive_242 string
        ,test_hive_243 string
        ,test_hive_245 string
        ,test_hive_246 string
        ,test_hive_247 string
        ,test_hive_248 string
        ,test_hive_249 string
        ,test_hive_250 string
        ,test_hive_218 string
        ,test_hive_219 string
        ,test_hive_220 string
        ,test_hive_221 string
        ,test_hive_222 string
        ,test_hive_223 string
        ,test_hive_224 string
        ,test_hive_226 string
        ,test_hive_227 string
        ,test_hive_228 string
        ,test_hive_229 string
        ,test_hive_230 string
        ,test_hive_231 string
        ,test_hive_232 string
        ,test_hive_233 string
        ,test_hive_234 string
        ,test_hive_235 string
        ,test_hive_236 string
        ,test_hive_237 string
        ,test_hive_238 string
        ,test_hive_239 string
        ,test_hive_1094 string
        ,test_hive_1096 string
        ,test_hive_1099 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1103
(
        test_hive_1097 string
        ,test_hive_1095 string
        ,test_hive_1098 string
        ,test_hive_1089 string
        ,test_hive_1090 string
        ,test_hive_10902 string
        ,test_hive_1092 string
        ,test_hive_1093 string
        ,test_hive_244 string
        ,test_hive_225 string
        ,test_hive_214 string
        ,test_hive_215 string
        ,test_hive_216 string
        ,test_hive_217 string
        ,test_hive_240 string
        ,test_hive_241 string
        ,test_hive_242 string
        ,test_hive_243 string
        ,test_hive_245 string
        ,test_hive_246 string
        ,test_hive_247 string
        ,test_hive_248 string
        ,test_hive_249 string
        ,test_hive_250 string
        ,test_hive_218 string
        ,test_hive_219 string
        ,test_hive_220 string
        ,test_hive_221 string
        ,test_hive_222 string
        ,test_hive_223 string
        ,test_hive_224 string
        ,test_hive_226 string
        ,test_hive_227 string
        ,test_hive_228 string
        ,test_hive_229 string
        ,test_hive_230 string
        ,test_hive_231 string
        ,test_hive_232 string
        ,test_hive_233 string
        ,test_hive_234 string
        ,test_hive_235 string
        ,test_hive_236 string
        ,test_hive_237 string
        ,test_hive_238 string
        ,test_hive_239 string
        ,test_hive_1094 string
        ,test_hive_1096 string
        ,test_hive_1099 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1102 purge;

create table if not exists test_hive_1102
(
max_partition bigint
);

drop view if exists test_hive_1105;

create view if not exists test_hive_1105
as
select
        cast(test_hive_1097 as int) as test_hive_1097
        ,cast(test_hive_1095 as int) as test_hive_1095
        ,cast(test_hive_1098 as int) as test_hive_1098
        ,cast(from_unixtime(unix_timestamp(test_hive_1089,'yyyymmdd'), 'yyyy-mm-dd') as timestamp) as test_hive_1089
        ,cast(test_hive_1090 as string) as test_hive_1090
        ,cast(test_hive_10902 as string) as test_hive_10902
        ,cast(test_hive_1092 as string) as test_hive_1092
        ,cast(test_hive_1093 as string) as test_hive_1093
        ,cast(test_hive_244 as string) as test_hive_244
        ,cast(test_hive_225 as string) as test_hive_225
        ,cast(test_hive_214 as string) as test_hive_214
        ,cast(from_unixtime(unix_timestamp(test_hive_215,'yyyymmdd'), 'yyyy-mm-dd') as timestamp) as test_hive_215
        ,cast(test_hive_216 as string) as test_hive_216
        ,cast(test_hive_217 as string) as test_hive_217
        ,cast(test_hive_240 as string) as test_hive_240
        ,cast(test_hive_241 as string) as test_hive_241
        ,cast(test_hive_242 as string) as test_hive_242
        ,cast(test_hive_243 as string) as test_hive_243
        ,cast(test_hive_245 as string) as test_hive_245
        ,cast(test_hive_246 as string) as test_hive_246
        ,cast(test_hive_247 as string) as test_hive_247
        ,cast(test_hive_248 as string) as test_hive_248
        ,cast(test_hive_249 as string) as test_hive_249
        ,cast(test_hive_250 as string) as test_hive_250
        ,cast(test_hive_218 as string) as test_hive_218
        ,cast(test_hive_219 as string) as test_hive_219
        ,cast(test_hive_220 as string) as test_hive_220
        ,cast(test_hive_221 as string) as test_hive_221
        ,cast(test_hive_222 as string) as test_hive_222
        ,cast(test_hive_223 as string) as test_hive_223
        ,cast(test_hive_224 as string) as test_hive_224
        ,cast(test_hive_226 as string) as test_hive_226
        ,cast(test_hive_227 as string) as test_hive_227
        ,cast(test_hive_228 as string) as test_hive_228
        ,cast(test_hive_229 as string) as test_hive_229
        ,cast(test_hive_230 as string) as test_hive_230
        ,cast(test_hive_231 as string) as test_hive_231
        ,cast(test_hive_232 as string) as test_hive_232
        ,cast(test_hive_233 as string) as test_hive_233
        ,cast(test_hive_234 as string) as test_hive_234
        ,cast(test_hive_235 as string) as test_hive_235
        ,cast(test_hive_236 as string) as test_hive_236
        ,cast(test_hive_237 as string) as test_hive_237
        ,cast(test_hive_238 as string) as test_hive_238
        ,cast(test_hive_239 as string) as test_hive_239
        ,cast(test_hive_1094 as int) as test_hive_1094
        ,cast(test_hive_1096 as int) as test_hive_1096
        ,cast(from_unixtime(unix_timestamp(test_hive_1099,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1099
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1103
;

drop view if exists test_hive_1104;

create view test_hive_1104
as
select
        test_hive_1097 as test_hive_1097
        ,test_hive_1095 as test_hive_1095
        ,test_hive_1098 as test_hive_1098
        ,test_hive_1089 as test_hive_1089
        ,test_hive_1090 as test_hive_1090
        ,test_hive_10902 as test_hive_10902
        ,test_hive_1092 as test_hive_1092
        ,test_hive_1093 as test_hive_1093
        ,test_hive_244 as test_hive_244
        ,test_hive_225 as test_hive_225
        ,test_hive_214 as test_hive_214
        ,test_hive_215 as test_hive_215
        ,test_hive_216 as test_hive_216
        ,test_hive_217 as test_hive_217
        ,test_hive_240 as test_hive_240
        ,test_hive_241 as test_hive_241
        ,test_hive_242 as test_hive_242
        ,test_hive_243 as test_hive_243
        ,test_hive_245 as test_hive_245
        ,test_hive_246 as test_hive_246
        ,test_hive_247 as test_hive_247
        ,test_hive_248 as test_hive_248
        ,test_hive_249 as test_hive_249
        ,test_hive_250 as test_hive_250
        ,test_hive_218 as test_hive_218
        ,test_hive_219 as test_hive_219
        ,test_hive_220 as test_hive_220
        ,test_hive_221 as test_hive_221
        ,test_hive_222 as test_hive_222
        ,test_hive_223 as test_hive_223
        ,test_hive_224 as test_hive_224
        ,test_hive_226 as test_hive_226
        ,test_hive_227 as test_hive_227
        ,test_hive_228 as test_hive_228
        ,test_hive_229 as test_hive_229
        ,test_hive_230 as test_hive_230
        ,test_hive_231 as test_hive_231
        ,test_hive_232 as test_hive_232
        ,test_hive_233 as test_hive_233
        ,test_hive_234 as test_hive_234
        ,test_hive_235 as test_hive_235
        ,test_hive_236 as test_hive_236
        ,test_hive_237 as test_hive_237
        ,test_hive_238 as test_hive_238
        ,test_hive_239 as test_hive_239
        ,test_hive_1094 as test_hive_1094
        ,test_hive_1096 as test_hive_1096
        ,test_hive_1099 as test_hive_1099
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
        ,ds
from test_hive_1105 t1
;

drop view if exists test_hive_1101;

create view test_hive_1101
as
select t1.*
from test_hive_1104 t1
inner join test_hive_1102 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1250 purge;

create table test_hive_1250
(
        test_hive_1240 string
        ,test_hive_1239 string
        ,test_hive_1241 string
        ,test_hive_300 string
        ,test_hive_288 string
        ,test_hive_294 string
        ,test_hive_299 string
        ,test_hive_297 string
        ,test_hive_285 string
        ,test_hive_292 string
        ,test_hive_290 string
        ,test_hive_291 string
        ,test_hive_303 string
        ,test_hive_304 string
        ,test_hive_305 string
        ,test_hive_286 string
        ,test_hive_293 string
        ,test_hive_287 string
        ,test_hive_296 string
        ,test_hive_295 string
        ,test_hive_298 string
        ,test_hive_282 string
        ,test_hive_283 string
        ,test_hive_284 string
        ,test_hive_289 string
        ,test_hive_302 string
        ,test_hive_301 string
        ,test_hive_281 string
        ,test_hive_1233 string
        ,test_hive_1234 string
        ,test_hive_12342 string
        ,test_hive_1236 string
        ,test_hive_1237 string
        ,test_hive_1238 string
        ,test_hive_1243 string
        ,test_hive_1243_lag string
        ,test_hive_1242 string
        ,test_hive_1232 string
        ,test_hive_1243_bp string
        ,test_hive_1243_lag_bp string
        ,test_hive_1243_con string
        ,test_hive_1243_lag_con string
        ,test_hive_1249 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1253
(
        test_hive_1240 string
        ,test_hive_1239 string
        ,test_hive_1241 string
        ,test_hive_300 string
        ,test_hive_288 string
        ,test_hive_294 string
        ,test_hive_299 string
        ,test_hive_297 string
        ,test_hive_285 string
        ,test_hive_292 string
        ,test_hive_290 string
        ,test_hive_291 string
        ,test_hive_303 string
        ,test_hive_304 string
        ,test_hive_305 string
        ,test_hive_286 string
        ,test_hive_293 string
        ,test_hive_287 string
        ,test_hive_296 string
        ,test_hive_295 string
        ,test_hive_298 string
        ,test_hive_282 string
        ,test_hive_283 string
        ,test_hive_284 string
        ,test_hive_289 string
        ,test_hive_302 string
        ,test_hive_301 string
        ,test_hive_281 string
        ,test_hive_1233 string
        ,test_hive_1234 string
        ,test_hive_12342 string
        ,test_hive_1236 string
        ,test_hive_1237 string
        ,test_hive_1238 string
        ,test_hive_1243 string
        ,test_hive_1243_lag string
        ,test_hive_1242 string
        ,test_hive_1232 string
        ,test_hive_1243_bp string
        ,test_hive_1243_lag_bp string
        ,test_hive_1243_con string
        ,test_hive_1243_lag_con string
        ,test_hive_1249 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1252 purge;

create table if not exists test_hive_1252
(
max_partition bigint
);

drop view if exists test_hive_1255;

create view if not exists test_hive_1255
as
select
        cast(test_hive_1240 as int) as test_hive_1240
        ,cast(test_hive_1239 as int) as test_hive_1239
        ,cast(test_hive_1241 as int) as test_hive_1241
        ,cast(test_hive_300 as string) as test_hive_300
        ,cast(test_hive_288 as string) as test_hive_288
        ,cast(test_hive_294 as string) as test_hive_294
        ,cast(test_hive_299 as string) as test_hive_299
        ,cast(test_hive_297 as string) as test_hive_297
        ,cast(test_hive_285 as string) as test_hive_285
        ,cast(test_hive_292 as string) as test_hive_292
        ,cast(test_hive_290 as string) as test_hive_290
        ,cast(test_hive_291 as string) as test_hive_291
        ,cast(test_hive_303 as string) as test_hive_303
        ,cast(test_hive_304 as string) as test_hive_304
        ,cast(test_hive_305 as string) as test_hive_305
        ,cast(test_hive_286 as string) as test_hive_286
        ,cast(test_hive_293 as string) as test_hive_293
        ,cast(test_hive_287 as string) as test_hive_287
        ,cast(test_hive_296 as string) as test_hive_296
        ,cast(test_hive_295 as string) as test_hive_295
        ,cast(test_hive_298 as string) as test_hive_298
        ,cast(test_hive_282 as string) as test_hive_282
        ,cast(test_hive_283 as string) as test_hive_283
        ,cast(test_hive_284 as string) as test_hive_284
        ,cast(test_hive_289 as string) as test_hive_289
        ,cast(test_hive_302 as string) as test_hive_302
        ,cast(test_hive_301 as string) as test_hive_301
        ,cast(test_hive_281 as string) as test_hive_281
        ,cast(from_unixtime(unix_timestamp(test_hive_1233,'yyyymmdd'), 'yyyy-mm-dd') as timestamp) as test_hive_1233
        ,cast(test_hive_1234 as string) as test_hive_1234
        ,cast(test_hive_12342 as string) as test_hive_12342
        ,cast(test_hive_1236 as string) as test_hive_1236
        ,cast(test_hive_1237 as string) as test_hive_1237
        ,cast(test_hive_1238 as string) as test_hive_1238
        ,cast(test_hive_1243 as double) as test_hive_1243
        ,cast(test_hive_1243_lag as double) as test_hive_1243_lag
        ,cast(test_hive_1242 as double) as test_hive_1242
        ,cast(test_hive_1232 as double) as test_hive_1232
        ,cast(test_hive_1243_bp as double) as test_hive_1243_bp
        ,cast(test_hive_1243_lag_bp as double) as test_hive_1243_lag_bp
        ,cast(test_hive_1243_con as double) as test_hive_1243_con
        ,cast(test_hive_1243_lag_con as double) as test_hive_1243_lag_con
        ,cast(from_unixtime(unix_timestamp(test_hive_1249,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1249
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1253
;

drop view if exists test_hive_1254;

create view test_hive_1254
as
select
        test_hive_1240 as test_hive_1240
        ,test_hive_1239 as test_hive_1239
        ,test_hive_1241 as test_hive_1241
        ,test_hive_300 as test_hive_300
        ,test_hive_288 as test_hive_288
        ,test_hive_294 as test_hive_294
        ,test_hive_299 as test_hive_299
        ,test_hive_297 as test_hive_297
        ,test_hive_285 as test_hive_285
        ,test_hive_292 as test_hive_292
        ,test_hive_290 as test_hive_290
        ,test_hive_291 as test_hive_291
        ,test_hive_303 as test_hive_303
        ,test_hive_304 as test_hive_304
        ,test_hive_305 as test_hive_305
        ,test_hive_286 as test_hive_286
        ,test_hive_293 as test_hive_293
        ,test_hive_287 as test_hive_287
        ,test_hive_296 as test_hive_296
        ,test_hive_295 as test_hive_295
        ,test_hive_298 as test_hive_298
        ,test_hive_282 as test_hive_282
        ,test_hive_283 as test_hive_283
        ,test_hive_284 as test_hive_284
        ,test_hive_289 as test_hive_289
        ,test_hive_302 as test_hive_302
        ,test_hive_301 as test_hive_301
        ,test_hive_281 as test_hive_281
        ,test_hive_1233 as test_hive_1233
        ,test_hive_1234 as test_hive_1234
        ,test_hive_12342 as test_hive_12342
        ,test_hive_1236 as test_hive_1236
        ,test_hive_1237 as test_hive_1237
        ,test_hive_1238 as test_hive_1238
        ,test_hive_1243 as test_hive_1243
        ,test_hive_1243_lag as test_hive_1243_lag
        ,test_hive_1242 as test_hive_1242
        ,test_hive_1232 as test_hive_1232
        ,test_hive_1243_bp as test_hive_1243_bp
        ,test_hive_1243_lag_bp as test_hive_1243_lag_bp
        ,test_hive_1243_con as test_hive_1243_con
        ,test_hive_1243_lag_con as test_hive_1243_lag_con
        ,test_hive_1249 as test_hive_1249
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
        ,ds
from test_hive_1255 t1
;

drop view if exists test_hive_1251;

create view test_hive_1251
as
select t1.*
from test_hive_1254 t1
inner join test_hive_1252 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1373 purge;

create table test_hive_1373
(
        test_hive_1370 string
        ,test_hive_1367 string
        ,test_hive_1371 string
        ,test_hive_1366 string
        ,test_hive_338 string
        ,test_hive_338_txt string
        ,test_hive_340 string
        ,test_hive_345 string
        ,test_hive_345_txt string
        ,test_hive_347 string
        ,test_hive_348 string
        ,test_hive_370 string
        ,test_hive_373 string
        ,test_hive_357 string
        ,test_hive_375 string
        ,test_hive_359 string
        ,test_hive_341 string
        ,test_hive_1368 string
        ,test_hive_1369 string
        ,test_hive_367 string
        ,test_hive_354 string
        ,test_hive_360 string
        ,test_hive_349 string
        ,test_hive_368 string
        ,test_hive_369 string
        ,test_hive_355 string
        ,test_hive_342 string
        ,test_hive_372 string
        ,test_hive_363 string
        ,test_hive_351 string
        ,test_hive_365 string
        ,test_hive_352 string
        ,test_hive_366 string
        ,test_hive_353 string
        ,test_hive_364 string
        ,test_hive_1381 string
        ,test_hive_358 string
        ,test_hive_1379 string
        ,test_hive_362 string
        ,test_hive_1380 string
        ,test_hive_361 string
        ,test_hive_350 string
        ,test_hive_374 string
        ,test_hive_343 string
        ,test_hive_343_txt string
        ,test_hive_371 string
        ,test_hive_356 string
        ,test_hive_1372 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1376
(
        test_hive_1370 string
        ,test_hive_1367 string
        ,test_hive_1371 string
        ,test_hive_1366 string
        ,test_hive_338 string
        ,test_hive_338_txt string
        ,test_hive_340 string
        ,test_hive_345 string
        ,test_hive_345_txt string
        ,test_hive_347 string
        ,test_hive_348 string
        ,test_hive_370 string
        ,test_hive_373 string
        ,test_hive_357 string
        ,test_hive_375 string
        ,test_hive_359 string
        ,test_hive_341 string
        ,test_hive_1368 string
        ,test_hive_1369 string
        ,test_hive_367 string
        ,test_hive_354 string
        ,test_hive_360 string
        ,test_hive_349 string
        ,test_hive_368 string
        ,test_hive_369 string
        ,test_hive_355 string
        ,test_hive_342 string
        ,test_hive_372 string
        ,test_hive_363 string
        ,test_hive_351 string
        ,test_hive_365 string
        ,test_hive_352 string
        ,test_hive_366 string
        ,test_hive_353 string
        ,test_hive_364 string
        ,test_hive_1381 string
        ,test_hive_358 string
        ,test_hive_1379 string
        ,test_hive_362 string
        ,test_hive_1380 string
        ,test_hive_361 string
        ,test_hive_350 string
        ,test_hive_374 string
        ,test_hive_343 string
        ,test_hive_343_txt string
        ,test_hive_371 string
        ,test_hive_356 string
        ,test_hive_1372 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1375 purge;

create table if not exists test_hive_1375
(
max_partition bigint
);

drop view if exists test_hive_1378;

create view if not exists test_hive_1378
as
select
        cast(test_hive_1370 as int) as test_hive_1370
        ,cast(test_hive_1367 as int) as test_hive_1367
        ,cast(test_hive_1371 as int) as test_hive_1371
        ,cast(test_hive_1366 as string) as test_hive_1366
        ,cast(test_hive_338 as string) as test_hive_338
        ,cast(test_hive_338_txt as string) as test_hive_338_txt
        ,cast(test_hive_340 as string) as test_hive_340
        ,cast(test_hive_345 as string) as test_hive_345
        ,cast(test_hive_345_txt as string) as test_hive_345_txt
        ,cast(test_hive_347 as string) as test_hive_347
        ,cast(test_hive_348 as string) as test_hive_348
        ,cast(test_hive_370 as string) as test_hive_370
        ,cast(test_hive_373 as string) as test_hive_373
        ,cast(test_hive_357 as string) as test_hive_357
        ,cast(test_hive_375 as string) as test_hive_375
        ,cast(test_hive_359 as string) as test_hive_359
        ,cast(test_hive_341 as string) as test_hive_341
        ,cast(test_hive_1368 as int) as test_hive_1368
        ,cast(test_hive_1369 as int) as test_hive_1369
        ,cast(test_hive_367 as string) as test_hive_367
        ,cast(test_hive_354 as string) as test_hive_354
        ,cast(test_hive_360 as string) as test_hive_360
        ,cast(test_hive_349 as string) as test_hive_349
        ,cast(test_hive_368 as string) as test_hive_368
        ,cast(test_hive_369 as string) as test_hive_369
        ,cast(test_hive_355 as string) as test_hive_355
        ,cast(from_unixtime(unix_timestamp(test_hive_342,'yyyymmdd'), 'yyyy-mm-dd') as timestamp) as test_hive_342
        ,cast(test_hive_372 as string) as test_hive_372
        ,cast(test_hive_363 as string) as test_hive_363
        ,cast(test_hive_351 as string) as test_hive_351
        ,cast(test_hive_365 as string) as test_hive_365
        ,cast(test_hive_352 as string) as test_hive_352
        ,cast(test_hive_366 as string) as test_hive_366
        ,cast(test_hive_353 as string) as test_hive_353
        ,cast(test_hive_364 as string) as test_hive_364
        ,cast(test_hive_1381 as string) as test_hive_1381
        ,cast(test_hive_358 as string) as test_hive_358
        ,cast(test_hive_1379 as string) as test_hive_1379
        ,cast(test_hive_362 as string) as test_hive_362
        ,cast(test_hive_1380 as string) as test_hive_1380
        ,cast(test_hive_361 as string) as test_hive_361
        ,cast(test_hive_350 as string) as test_hive_350
        ,cast(test_hive_374 as string) as test_hive_374
        ,cast(test_hive_343 as string) as test_hive_343
        ,cast(test_hive_343_txt as string) as test_hive_343_txt
        ,cast(test_hive_371 as string) as test_hive_371
        ,cast(test_hive_356 as string) as test_hive_356
        ,cast(from_unixtime(unix_timestamp(test_hive_1372,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1372
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1376
;

drop view if exists test_hive_1377;

create view test_hive_1377
as
select
        test_hive_1370 as test_hive_1370
        ,test_hive_1367 as test_hive_1367
        ,test_hive_1371 as test_hive_1371
        ,test_hive_1366 as test_hive_1366
        ,test_hive_338 as test_hive_338
        ,test_hive_338_txt as test_hive_338_txt
        ,test_hive_340 as test_hive_340
        ,test_hive_345 as test_hive_345
        ,test_hive_345_txt as test_hive_345_txt
        ,test_hive_347 as test_hive_347
        ,test_hive_348 as test_hive_348
        ,test_hive_370 as test_hive_370
        ,test_hive_373 as test_hive_373
        ,test_hive_357 as test_hive_357
        ,test_hive_375 as test_hive_375
        ,test_hive_359 as test_hive_359
        ,test_hive_341 as test_hive_341
        ,test_hive_1368 as test_hive_1368
        ,test_hive_1369 as test_hive_1369
        ,test_hive_367 as test_hive_367
        ,test_hive_354 as test_hive_354
        ,test_hive_360 as test_hive_360
        ,test_hive_349 as test_hive_349
        ,test_hive_368 as test_hive_368
        ,test_hive_369 as test_hive_369
        ,test_hive_355 as test_hive_355
        ,test_hive_342 as test_hive_342
        ,test_hive_372 as test_hive_372
        ,test_hive_363 as test_hive_363
        ,test_hive_351 as test_hive_351
        ,test_hive_365 as test_hive_365
        ,test_hive_352 as test_hive_352
        ,test_hive_366 as test_hive_366
        ,test_hive_353 as test_hive_353
        ,test_hive_364 as test_hive_364
        ,test_hive_1381 as test_hive_1381
        ,test_hive_358 as test_hive_358
        ,test_hive_1379 as test_hive_1379
        ,test_hive_362 as test_hive_362
        ,test_hive_1380 as test_hive_1380
        ,test_hive_361 as test_hive_361
        ,test_hive_350 as test_hive_350
        ,test_hive_374 as test_hive_374
        ,test_hive_343 as test_hive_343
        ,test_hive_343_txt as test_hive_343_txt
        ,test_hive_371 as test_hive_371
        ,test_hive_356 as test_hive_356
        ,test_hive_1372 as test_hive_1372
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
        ,ds
from test_hive_1378 t1
;

drop view if exists test_hive_1374;

create view test_hive_1374
as
select t1.*
from test_hive_1377 t1
inner join test_hive_1375 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1397 purge;

create table test_hive_1397
(
        test_hive_1394 string
        ,test_hive_1383 string
        ,test_hive_1395 string
        ,test_hive_1382 string
        ,test_hive_407 string
        ,test_hive_397 string
        ,test_hive_379 string
        ,test_hive_408 string
        ,test_hive_1384 string
        ,test_hive_1385 string
        ,test_hive_1386 string
        ,test_hive_1387 string
        ,test_hive_1388 string
        ,test_hive_1389 string
        ,test_hive_1390 string
        ,test_hive_1391 string
        ,test_hive_1392 string
        ,test_hive_1393 string
        ,test_hive_400 string
        ,test_hive_386 string
        ,test_hive_409 string
        ,test_hive_390 string
        ,test_hive_381 string
        ,test_hive_380 string
        ,test_hive_382 string
        ,test_hive_382_txt string
        ,test_hive_410 string
        ,test_hive_391 string
        ,test_hive_403 string
        ,test_hive_388 string
        ,test_hive_405 string
        ,test_hive_389 string
        ,test_hive_393 string
        ,test_hive_376 string
        ,test_hive_394 string
        ,test_hive_377 string
        ,test_hive_395 string
        ,test_hive_378 string
        ,test_hive_406 string
        ,test_hive_1406 string
        ,test_hive_404 string
        ,test_hive_1405 string
        ,test_hive_396 string
        ,test_hive_1403 string
        ,test_hive_402 string
        ,test_hive_1404 string
        ,test_hive_398 string
        ,test_hive_384 string
        ,test_hive_399 string
        ,test_hive_385 string
        ,test_hive_411 string
        ,test_hive_392 string
        ,test_hive_401 string
        ,test_hive_387 string
        ,test_hive_1396 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1400
(
        test_hive_1394 string
        ,test_hive_1383 string
        ,test_hive_1395 string
        ,test_hive_1382 string
        ,test_hive_407 string
        ,test_hive_397 string
        ,test_hive_379 string
        ,test_hive_408 string
        ,test_hive_1384 string
        ,test_hive_1385 string
        ,test_hive_1386 string
        ,test_hive_1387 string
        ,test_hive_1388 string
        ,test_hive_1389 string
        ,test_hive_1390 string
        ,test_hive_1391 string
        ,test_hive_1392 string
        ,test_hive_1393 string
        ,test_hive_400 string
        ,test_hive_386 string
        ,test_hive_409 string
        ,test_hive_390 string
        ,test_hive_381 string
        ,test_hive_380 string
        ,test_hive_382 string
        ,test_hive_382_txt string
        ,test_hive_410 string
        ,test_hive_391 string
        ,test_hive_403 string
        ,test_hive_388 string
        ,test_hive_405 string
        ,test_hive_389 string
        ,test_hive_393 string
        ,test_hive_376 string
        ,test_hive_394 string
        ,test_hive_377 string
        ,test_hive_395 string
        ,test_hive_378 string
        ,test_hive_406 string
        ,test_hive_1406 string
        ,test_hive_404 string
        ,test_hive_1405 string
        ,test_hive_396 string
        ,test_hive_1403 string
        ,test_hive_402 string
        ,test_hive_1404 string
        ,test_hive_398 string
        ,test_hive_384 string
        ,test_hive_399 string
        ,test_hive_385 string
        ,test_hive_411 string
        ,test_hive_392 string
        ,test_hive_401 string
        ,test_hive_387 string
        ,test_hive_1396 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1399 purge;

create table if not exists test_hive_1399
(
max_partition bigint
);

drop view if exists test_hive_1402;

create view if not exists test_hive_1402
as
select
        cast(test_hive_1394 as int) as test_hive_1394
        ,cast(test_hive_1383 as int) as test_hive_1383
        ,cast(test_hive_1395 as int) as test_hive_1395
        ,cast(test_hive_1382 as string) as test_hive_1382
        ,cast(test_hive_407 as string) as test_hive_407
        ,cast(test_hive_397 as string) as test_hive_397
        ,cast(test_hive_379 as string) as test_hive_379
        ,cast(test_hive_408 as string) as test_hive_408
        ,cast(test_hive_1384 as int) as test_hive_1384
        ,cast(test_hive_1385 as int) as test_hive_1385
        ,cast(test_hive_1386 as int) as test_hive_1386
        ,cast(test_hive_1387 as int) as test_hive_1387
        ,cast(test_hive_1388 as int) as test_hive_1388
        ,cast(test_hive_1389 as double) as test_hive_1389
        ,cast(test_hive_1390 as double) as test_hive_1390
        ,cast(test_hive_1391 as int) as test_hive_1391
        ,cast(test_hive_1392 as int) as test_hive_1392
        ,cast(test_hive_1393 as int) as test_hive_1393
        ,cast(test_hive_400 as string) as test_hive_400
        ,cast(test_hive_386 as string) as test_hive_386
        ,cast(test_hive_409 as string) as test_hive_409
        ,cast(test_hive_390 as string) as test_hive_390
        ,cast(from_unixtime(unix_timestamp(test_hive_381,'yyyymmdd'), 'yyyy-mm-dd') as timestamp) as test_hive_381
        ,cast(test_hive_380 as string) as test_hive_380
        ,cast(test_hive_382 as string) as test_hive_382
        ,cast(test_hive_382_txt as string) as test_hive_382_txt
        ,cast(test_hive_410 as string) as test_hive_410
        ,cast(test_hive_391 as string) as test_hive_391
        ,cast(test_hive_403 as string) as test_hive_403
        ,cast(test_hive_388 as string) as test_hive_388
        ,cast(test_hive_405 as string) as test_hive_405
        ,cast(test_hive_389 as string) as test_hive_389
        ,cast(test_hive_393 as string) as test_hive_393
        ,cast(test_hive_376 as string) as test_hive_376
        ,cast(test_hive_394 as string) as test_hive_394
        ,cast(test_hive_377 as string) as test_hive_377
        ,cast(test_hive_395 as string) as test_hive_395
        ,cast(test_hive_378 as string) as test_hive_378
        ,cast(test_hive_406 as string) as test_hive_406
        ,cast(test_hive_1406 as string) as test_hive_1406
        ,cast(test_hive_404 as string) as test_hive_404
        ,cast(test_hive_1405 as string) as test_hive_1405
        ,cast(test_hive_396 as string) as test_hive_396
        ,cast(test_hive_1403 as string) as test_hive_1403
        ,cast(test_hive_402 as string) as test_hive_402
        ,cast(test_hive_1404 as string) as test_hive_1404
        ,cast(test_hive_398 as string) as test_hive_398
        ,cast(test_hive_384 as string) as test_hive_384
        ,cast(test_hive_399 as string) as test_hive_399
        ,cast(test_hive_385 as string) as test_hive_385
        ,cast(test_hive_411 as string) as test_hive_411
        ,cast(test_hive_392 as string) as test_hive_392
        ,cast(test_hive_401 as string) as test_hive_401
        ,cast(test_hive_387 as string) as test_hive_387
        ,cast(from_unixtime(unix_timestamp(test_hive_1396,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1396
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1400
;

drop view if exists test_hive_1401;

create view test_hive_1401
as
select
        test_hive_1394 as test_hive_1394
        ,test_hive_1383 as test_hive_1383
        ,test_hive_1395 as test_hive_1395
        ,test_hive_1382 as test_hive_1382
        ,test_hive_407 as test_hive_407
        ,test_hive_397 as test_hive_397
        ,test_hive_379 as test_hive_379
        ,test_hive_408 as test_hive_408
        ,test_hive_1384 as test_hive_1384
        ,test_hive_1385 as test_hive_1385
        ,test_hive_1386 as test_hive_1386
        ,test_hive_1387 as test_hive_1387
        ,test_hive_1388 as test_hive_1388
        ,test_hive_1389 as test_hive_1389
        ,test_hive_1390 as test_hive_1390
        ,test_hive_1391 as test_hive_1391
        ,test_hive_1392 as test_hive_1392
        ,test_hive_1393 as test_hive_1393
        ,test_hive_400 as test_hive_400
        ,test_hive_386 as test_hive_386
        ,test_hive_409 as test_hive_409
        ,test_hive_390 as test_hive_390
        ,test_hive_381 as test_hive_381
        ,test_hive_380 as test_hive_380
        ,test_hive_382 as test_hive_382
        ,test_hive_382_txt as test_hive_382_txt
        ,test_hive_410 as test_hive_410
        ,test_hive_391 as test_hive_391
        ,test_hive_403 as test_hive_403
        ,test_hive_388 as test_hive_388
        ,test_hive_405 as test_hive_405
        ,test_hive_389 as test_hive_389
        ,test_hive_393 as test_hive_393
        ,test_hive_376 as test_hive_376
        ,test_hive_394 as test_hive_394
        ,test_hive_377 as test_hive_377
        ,test_hive_395 as test_hive_395
        ,test_hive_378 as test_hive_378
        ,test_hive_406 as test_hive_406
        ,test_hive_1406 as test_hive_1406
        ,test_hive_404 as test_hive_404
        ,test_hive_1405 as test_hive_1405
        ,test_hive_396 as test_hive_396
        ,test_hive_1403 as test_hive_1403
        ,test_hive_402 as test_hive_402
        ,test_hive_1404 as test_hive_1404
        ,test_hive_398 as test_hive_398
        ,test_hive_384 as test_hive_384
        ,test_hive_399 as test_hive_399
        ,test_hive_385 as test_hive_385
        ,test_hive_411 as test_hive_411
        ,test_hive_392 as test_hive_392
        ,test_hive_401 as test_hive_401
        ,test_hive_387 as test_hive_387
        ,test_hive_1396 as test_hive_1396
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
        ,ds
from test_hive_1402 t1
;

drop view if exists test_hive_1398;

create view test_hive_1398
as
select t1.*
from test_hive_1401 t1
inner join test_hive_1399 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1417 purge;

create table test_hive_1417
(
        test_hive_1411 string
        ,test_hive_1407 string
        ,test_hive_1412 string
        ,test_hive_412 string
        ,test_hive_1410 string
        ,test_hive_1409 string
        ,test_hive_1408 string
        ,test_hive_1415 string
        ,test_hive_1414 string
        ,test_hive_1413 string
        ,test_hive_1416 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1420
(
        test_hive_1411 string
        ,test_hive_1407 string
        ,test_hive_1412 string
        ,test_hive_412 string
        ,test_hive_1410 string
        ,test_hive_1409 string
        ,test_hive_1408 string
        ,test_hive_1415 string
        ,test_hive_1414 string
        ,test_hive_1413 string
        ,test_hive_1416 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1419 purge;

create table if not exists test_hive_1419
(
max_partition bigint
);

drop view if exists test_hive_1422;

create view if not exists test_hive_1422
as
select
        cast(test_hive_1411 as int) as test_hive_1411
        ,cast(test_hive_1407 as int) as test_hive_1407
        ,cast(test_hive_1412 as int) as test_hive_1412
        ,cast(test_hive_412 as string) as test_hive_412
        ,cast(test_hive_1410 as string) as test_hive_1410
        ,cast(from_unixtime(unix_timestamp(test_hive_1409,'yyyymmdd'), 'yyyy-mm-dd') as timestamp) as test_hive_1409
        ,cast(from_unixtime(unix_timestamp(test_hive_1408,'yyyymmdd'), 'yyyy-mm-dd') as timestamp) as test_hive_1408
        ,cast(test_hive_1415 as string) as test_hive_1415
        ,cast(test_hive_1414 as string) as test_hive_1414
        ,cast(test_hive_1413 as string) as test_hive_1413
        ,cast(from_unixtime(unix_timestamp(test_hive_1416,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1416
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1420
;

drop view if exists test_hive_1421;

create view test_hive_1421
as
select
        test_hive_1411 as test_hive_1411
        ,test_hive_1407 as test_hive_1407
        ,test_hive_1412 as test_hive_1412
        ,test_hive_412 as test_hive_412
        ,test_hive_1410 as test_hive_1410
        ,test_hive_1409 as test_hive_1409
        ,test_hive_1408 as test_hive_1408
        ,test_hive_1415 as test_hive_1415
        ,test_hive_1414 as test_hive_1414
        ,test_hive_1413 as test_hive_1413
        ,test_hive_1416 as test_hive_1416
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1422 t1
;

drop view if exists test_hive_1418;

create view test_hive_1418
as
select t1.*
from test_hive_1421 t1
inner join test_hive_1419 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1114 purge;

create table test_hive_1114
(
        test_hive_1108 string
        ,test_hive_1106 string
        ,test_hive_1109 string
        ,test_hive_272 string
        ,test_hive_1107 string
        ,test_hive_1112 string
        ,test_hive_1111 string
        ,test_hive_1110 string
        ,test_hive_1113 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1117
(
        test_hive_1108 string
        ,test_hive_1106 string
        ,test_hive_1109 string
        ,test_hive_272 string
        ,test_hive_1107 string
        ,test_hive_1112 string
        ,test_hive_1111 string
        ,test_hive_1110 string
        ,test_hive_1113 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1116 purge;

create table if not exists test_hive_1116
(
max_partition bigint
);

drop view if exists test_hive_1119;

create view if not exists test_hive_1119
as
select
        cast(test_hive_1108 as int) as test_hive_1108
        ,cast(test_hive_1106 as int) as test_hive_1106
        ,cast(test_hive_1109 as int) as test_hive_1109
        ,cast(test_hive_272 as string) as test_hive_272
        ,cast(test_hive_1107 as string) as test_hive_1107
        ,cast(test_hive_1112 as string) as test_hive_1112
        ,cast(test_hive_1111 as string) as test_hive_1111
        ,cast(test_hive_1110 as string) as test_hive_1110
        ,cast(from_unixtime(unix_timestamp(test_hive_1113,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1113
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1117
;

drop view if exists test_hive_1118;

create view test_hive_1118
as
select
        test_hive_1108 as test_hive_1108
        ,test_hive_1106 as test_hive_1106
        ,test_hive_1109 as test_hive_1109
        ,test_hive_272 as test_hive_272
        ,test_hive_1107 as test_hive_1107
        ,test_hive_1112 as test_hive_1112
        ,test_hive_1111 as test_hive_1111
        ,test_hive_1110 as test_hive_1110
        ,test_hive_1113 as test_hive_1113
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
        ,ds
from test_hive_1119 t1
;

drop view if exists test_hive_1115;

create view test_hive_1115
as
select t1.*
from test_hive_1118 t1
inner join test_hive_1116 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1128 purge;

create table test_hive_1128
(
        test_hive_1122 string
        ,test_hive_1120 string
        ,test_hive_1123 string
        ,test_hive_273 string
        ,test_hive_1121 string
        ,test_hive_1126 string
        ,test_hive_1125 string
        ,test_hive_1124 string
        ,test_hive_1127 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1131
(
        test_hive_1122 string
        ,test_hive_1120 string
        ,test_hive_1123 string
        ,test_hive_273 string
        ,test_hive_1121 string
        ,test_hive_1126 string
        ,test_hive_1125 string
        ,test_hive_1124 string
        ,test_hive_1127 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1130 purge;

create table if not exists test_hive_1130
(
max_partition bigint
);

drop view if exists test_hive_1133;

create view if not exists test_hive_1133
as
select
        cast(test_hive_1122 as int) as test_hive_1122
        ,cast(test_hive_1120 as int) as test_hive_1120
        ,cast(test_hive_1123 as int) as test_hive_1123
        ,cast(test_hive_273 as string) as test_hive_273
        ,cast(test_hive_1121 as string) as test_hive_1121
        ,cast(test_hive_1126 as string) as test_hive_1126
        ,cast(test_hive_1125 as string) as test_hive_1125
        ,cast(test_hive_1124 as string) as test_hive_1124
        ,cast(from_unixtime(unix_timestamp(test_hive_1127,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1127
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1131
;

drop view if exists test_hive_1132;

create view test_hive_1132
as
select
        test_hive_1122 as test_hive_1122
        ,test_hive_1120 as test_hive_1120
        ,test_hive_1123 as test_hive_1123
        ,test_hive_273 as test_hive_273
        ,test_hive_1121 as test_hive_1121
        ,test_hive_1126 as test_hive_1126
        ,test_hive_1125 as test_hive_1125
        ,test_hive_1124 as test_hive_1124
        ,test_hive_1127 as test_hive_1127
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
        ,ds
from test_hive_1133 t1
;

drop view if exists test_hive_1129;

create view test_hive_1129
as
select t1.*
from test_hive_1132 t1
inner join test_hive_1130 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1142 purge;

create table test_hive_1142
(
        test_hive_1136 string
        ,test_hive_1134 string
        ,test_hive_1137 string
        ,test_hive_274 string
        ,test_hive_1135 string
        ,test_hive_1140 string
        ,test_hive_1139 string
        ,test_hive_1138 string
        ,test_hive_1141 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1145
(
        test_hive_1136 string
        ,test_hive_1134 string
        ,test_hive_1137 string
        ,test_hive_274 string
        ,test_hive_1135 string
        ,test_hive_1140 string
        ,test_hive_1139 string
        ,test_hive_1138 string
        ,test_hive_1141 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1144 purge;

create table if not exists test_hive_1144
(
max_partition bigint
);

drop view if exists test_hive_1147;

create view if not exists test_hive_1147
as
select
        cast(test_hive_1136 as int) as test_hive_1136
        ,cast(test_hive_1134 as int) as test_hive_1134
        ,cast(test_hive_1137 as int) as test_hive_1137
        ,cast(test_hive_274 as string) as test_hive_274
        ,cast(test_hive_1135 as string) as test_hive_1135
        ,cast(test_hive_1140 as string) as test_hive_1140
        ,cast(test_hive_1139 as string) as test_hive_1139
        ,cast(test_hive_1138 as string) as test_hive_1138
        ,cast(from_unixtime(unix_timestamp(test_hive_1141,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1141
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1145
;

drop view if exists test_hive_1146;

create view test_hive_1146
as
select
        test_hive_1136 as test_hive_1136
        ,test_hive_1134 as test_hive_1134
        ,test_hive_1137 as test_hive_1137
        ,test_hive_274 as test_hive_274
        ,test_hive_1135 as test_hive_1135
        ,test_hive_1140 as test_hive_1140
        ,test_hive_1139 as test_hive_1139
        ,test_hive_1138 as test_hive_1138
        ,test_hive_1141 as test_hive_1141
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1147 t1
;

drop view if exists test_hive_1143;

create view test_hive_1143
as
select t1.*
from test_hive_1146 t1
inner join test_hive_1144 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1158 purge;

create table test_hive_1158
(
        test_hive_1152 string
        ,test_hive_1148 string
        ,test_hive_1153 string
        ,test_hive_275 string
        ,test_hive_1151 string
        ,test_hive_1150 string
        ,test_hive_1149 string
        ,test_hive_1156 string
        ,test_hive_1155 string
        ,test_hive_1154 string
        ,test_hive_1157 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1161
(
        test_hive_1152 string
        ,test_hive_1148 string
        ,test_hive_1153 string
        ,test_hive_275 string
        ,test_hive_1151 string
        ,test_hive_1150 string
        ,test_hive_1149 string
        ,test_hive_1156 string
        ,test_hive_1155 string
        ,test_hive_1154 string
        ,test_hive_1157 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1160 purge;

create table if not exists test_hive_1160
(
max_partition bigint
);

drop view if exists test_hive_1163;

create view if not exists test_hive_1163
as
select
        cast(test_hive_1152 as int) as test_hive_1152
        ,cast(test_hive_1148 as int) as test_hive_1148
        ,cast(test_hive_1153 as int) as test_hive_1153
        ,cast(test_hive_275 as decimal) as test_hive_275
        ,cast(test_hive_1151 as string) as test_hive_1151
        ,cast(from_unixtime(unix_timestamp(test_hive_1150,'yyyymmdd'), 'yyyy-mm-dd') as timestamp) as test_hive_1150
        ,cast(from_unixtime(unix_timestamp(test_hive_1149,'yyyymmdd'), 'yyyy-mm-dd') as timestamp) as test_hive_1149
        ,cast(test_hive_1156 as string) as test_hive_1156
        ,cast(test_hive_1155 as string) as test_hive_1155
        ,cast(test_hive_1154 as string) as test_hive_1154
        ,cast(from_unixtime(unix_timestamp(test_hive_1157,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1157
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1161
;

drop view if exists test_hive_1162;

create view test_hive_1162
as
select
        test_hive_1152 as test_hive_1152
        ,test_hive_1148 as test_hive_1148
        ,test_hive_1153 as test_hive_1153
        ,test_hive_275 as test_hive_275
        ,test_hive_1151 as test_hive_1151
        ,test_hive_1150 as test_hive_1150
        ,test_hive_1149 as test_hive_1149
        ,test_hive_1156 as test_hive_1156
        ,test_hive_1155 as test_hive_1155
        ,test_hive_1154 as test_hive_1154
        ,test_hive_1157 as test_hive_1157
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
        ,ds
from test_hive_1163 t1
;

drop view if exists test_hive_1159;

create view test_hive_1159
as
select t1.*
from test_hive_1162 t1
inner join test_hive_1160 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1172 purge;

create table test_hive_1172
(
        test_hive_1166 string
        ,test_hive_1164 string
        ,test_hive_1167 string
        ,test_hive_276 string
        ,test_hive_1165 string
        ,test_hive_1170 string
        ,test_hive_1169 string
        ,test_hive_1168 string
        ,test_hive_1171 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1175
(
        test_hive_1166 string
        ,test_hive_1164 string
        ,test_hive_1167 string
        ,test_hive_276 string
        ,test_hive_1165 string
        ,test_hive_1170 string
        ,test_hive_1169 string
        ,test_hive_1168 string
        ,test_hive_1171 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1174 purge;

create table if not exists test_hive_1174
(
max_partition bigint
);

drop view if exists test_hive_1177;

create view if not exists test_hive_1177
as
select
        cast(test_hive_1166 as int) as test_hive_1166
        ,cast(test_hive_1164 as int) as test_hive_1164
        ,cast(test_hive_1167 as int) as test_hive_1167
        ,cast(test_hive_276 as string) as test_hive_276
        ,cast(test_hive_1165 as string) as test_hive_1165
        ,cast(test_hive_1170 as string) as test_hive_1170
        ,cast(test_hive_1169 as string) as test_hive_1169
        ,cast(test_hive_1168 as string) as test_hive_1168
        ,cast(from_unixtime(unix_timestamp(test_hive_1171,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1171
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1175
;

drop view if exists test_hive_1176;

create view test_hive_1176
as
select
        test_hive_1166 as test_hive_1166
        ,test_hive_1164 as test_hive_1164
        ,test_hive_1167 as test_hive_1167
        ,test_hive_276 as test_hive_276
        ,test_hive_1165 as test_hive_1165
        ,test_hive_1170 as test_hive_1170
        ,test_hive_1169 as test_hive_1169
        ,test_hive_1168 as test_hive_1168
        ,test_hive_1171 as test_hive_1171
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1177 t1
;

drop view if exists test_hive_1173;

create view test_hive_1173
as
select t1.*
from test_hive_1176 t1
inner join test_hive_1174 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1186 purge;

create table test_hive_1186
(
        test_hive_1180 string
        ,test_hive_1178 string
        ,test_hive_1181 string
        ,test_hive_277 string
        ,test_hive_1179 string
        ,test_hive_1184 string
        ,test_hive_1183 string
        ,test_hive_1182 string
        ,test_hive_1185 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1189
(
        test_hive_1180 string
        ,test_hive_1178 string
        ,test_hive_1181 string
        ,test_hive_277 string
        ,test_hive_1179 string
        ,test_hive_1184 string
        ,test_hive_1183 string
        ,test_hive_1182 string
        ,test_hive_1185 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1188 purge;

create table if not exists test_hive_1188
(
max_partition bigint
);

drop view if exists test_hive_1191;

create view if not exists test_hive_1191
as
select
        cast(test_hive_1180 as int) as test_hive_1180
        ,cast(test_hive_1178 as int) as test_hive_1178
        ,cast(test_hive_1181 as int) as test_hive_1181
        ,cast(test_hive_277 as string) as test_hive_277
        ,cast(test_hive_1179 as string) as test_hive_1179
        ,cast(test_hive_1184 as string) as test_hive_1184
        ,cast(test_hive_1183 as string) as test_hive_1183
        ,cast(test_hive_1182 as string) as test_hive_1182
        ,cast(from_unixtime(unix_timestamp(test_hive_1185,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1185
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1189
;

drop view if exists test_hive_1190;

create view test_hive_1190
as
select
        test_hive_1180 as test_hive_1180
        ,test_hive_1178 as test_hive_1178
        ,test_hive_1181 as test_hive_1181
        ,test_hive_277 as test_hive_277
        ,test_hive_1179 as test_hive_1179
        ,test_hive_1184 as test_hive_1184
        ,test_hive_1183 as test_hive_1183
        ,test_hive_1182 as test_hive_1182
        ,test_hive_1185 as test_hive_1185
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
        ,ds
from test_hive_1191 t1
;

drop view if exists test_hive_1187;

create view test_hive_1187
as
select t1.*
from test_hive_1190 t1
inner join test_hive_1188 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1202 purge;

create table test_hive_1202
(
        test_hive_1196 string
        ,test_hive_1192 string
        ,test_hive_1197 string
        ,test_hive_278 string
        ,test_hive_1195 string
        ,test_hive_1194 string
        ,test_hive_1193 string
        ,test_hive_1200 string
        ,test_hive_1199 string
        ,test_hive_1198 string
        ,test_hive_1201 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1205
(
        test_hive_1196 string
        ,test_hive_1192 string
        ,test_hive_1197 string
        ,test_hive_278 string
        ,test_hive_1195 string
        ,test_hive_1194 string
        ,test_hive_1193 string
        ,test_hive_1200 string
        ,test_hive_1199 string
        ,test_hive_1198 string
        ,test_hive_1201 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1204 purge;

create table if not exists test_hive_1204
(
max_partition bigint
);

drop view if exists test_hive_1207;

create view if not exists test_hive_1207
as
select
        cast(test_hive_1196 as int) as test_hive_1196
        ,cast(test_hive_1192 as int) as test_hive_1192
        ,cast(test_hive_1197 as int) as test_hive_1197
        ,cast(test_hive_278 as decimal) as test_hive_278
        ,cast(test_hive_1195 as string) as test_hive_1195
        ,cast(from_unixtime(unix_timestamp(test_hive_1194,'yyyymmdd'), 'yyyy-mm-dd') as timestamp) as test_hive_1194
        ,cast(from_unixtime(unix_timestamp(test_hive_1193,'yyyymmdd'), 'yyyy-mm-dd') as timestamp) as test_hive_1193
        ,cast(test_hive_1200 as string) as test_hive_1200
        ,cast(test_hive_1199 as string) as test_hive_1199
        ,cast(test_hive_1198 as string) as test_hive_1198
        ,cast(from_unixtime(unix_timestamp(test_hive_1201,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1201
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1205
;

drop view if exists test_hive_1206;

create view test_hive_1206
as
select
        test_hive_1196 as test_hive_1196
        ,test_hive_1192 as test_hive_1192
        ,test_hive_1197 as test_hive_1197
        ,test_hive_278 as test_hive_278
        ,test_hive_1195 as test_hive_1195
        ,test_hive_1194 as test_hive_1194
        ,test_hive_1193 as test_hive_1193
        ,test_hive_1200 as test_hive_1200
        ,test_hive_1199 as test_hive_1199
        ,test_hive_1198 as test_hive_1198
        ,test_hive_1201 as test_hive_1201
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1207 t1
;

drop view if exists test_hive_1203;

create view test_hive_1203
as
select t1.*
from test_hive_1206 t1
inner join test_hive_1204 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1312 purge;

create table test_hive_1312
(
        test_hive_1307 string
        ,test_hive_1305 string
        ,test_hive_1308 string
        ,test_hive_334 string
        ,test_hive_1306 string
        ,test_hive_1310 string
        ,test_hive_1309 string
        ,test_hive_1311 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1315
(
        test_hive_1307 string
        ,test_hive_1305 string
        ,test_hive_1308 string
        ,test_hive_334 string
        ,test_hive_1306 string
        ,test_hive_1310 string
        ,test_hive_1309 string
        ,test_hive_1311 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1314 purge;

create table if not exists test_hive_1314
(
max_partition bigint
);

drop view if exists test_hive_1317;

create view if not exists test_hive_1317
as
select
        cast(test_hive_1307 as int) as test_hive_1307
        ,cast(test_hive_1305 as int) as test_hive_1305
        ,cast(test_hive_1308 as int) as test_hive_1308
        ,cast(test_hive_334 as string) as test_hive_334
        ,cast(test_hive_1306 as string) as test_hive_1306
        ,cast(test_hive_1310 as string) as test_hive_1310
        ,cast(test_hive_1309 as string) as test_hive_1309
        ,cast(from_unixtime(unix_timestamp(test_hive_1311,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1311
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1315
;

drop view if exists test_hive_1316;

create view test_hive_1316
as
select
        test_hive_1307 as test_hive_1307
        ,test_hive_1305 as test_hive_1305
        ,test_hive_1308 as test_hive_1308
        ,test_hive_334 as test_hive_334
        ,test_hive_1306 as test_hive_1306
        ,test_hive_1310 as test_hive_1310
        ,test_hive_1309 as test_hive_1309
        ,test_hive_1311 as test_hive_1311
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
        ,ds
from test_hive_1317 t1
;

drop view if exists test_hive_1313;

create view test_hive_1313
as
select t1.*
from test_hive_1316 t1
inner join test_hive_1314 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1344 purge;

create table test_hive_1344
(
        test_hive_1338 string
        ,test_hive_1334 string
        ,test_hive_1339 string
        ,test_hive_336 string
        ,test_hive_1337 string
        ,test_hive_1336 string
        ,test_hive_1335 string
        ,test_hive_1342 string
        ,test_hive_1341 string
        ,test_hive_1340 string
        ,test_hive_1343 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1347
(
        test_hive_1338 string
        ,test_hive_1334 string
        ,test_hive_1339 string
        ,test_hive_336 string
        ,test_hive_1337 string
        ,test_hive_1336 string
        ,test_hive_1335 string
        ,test_hive_1342 string
        ,test_hive_1341 string
        ,test_hive_1340 string
        ,test_hive_1343 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1346 purge;

create table if not exists test_hive_1346
(
max_partition bigint
);

drop view if exists test_hive_1349;

create view if not exists test_hive_1349
as
select
        cast(test_hive_1338 as int) as test_hive_1338
        ,cast(test_hive_1334 as int) as test_hive_1334
        ,cast(test_hive_1339 as int) as test_hive_1339
        ,cast(test_hive_336 as string) as test_hive_336
        ,cast(test_hive_1337 as string) as test_hive_1337
        ,cast(from_unixtime(unix_timestamp(test_hive_1336,'yyyymmdd'), 'yyyy-mm-dd') as timestamp) as test_hive_1336
        ,cast(from_unixtime(unix_timestamp(test_hive_1335,'yyyymmdd'), 'yyyy-mm-dd') as timestamp) as test_hive_1335
        ,cast(test_hive_1342 as string) as test_hive_1342
        ,cast(test_hive_1341 as string) as test_hive_1341
        ,cast(test_hive_1340 as string) as test_hive_1340
        ,cast(from_unixtime(unix_timestamp(test_hive_1343,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1343
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1347
;

drop view if exists test_hive_1348;

create view test_hive_1348
as
select
        test_hive_1338 as test_hive_1338
        ,test_hive_1334 as test_hive_1334
        ,test_hive_1339 as test_hive_1339
        ,test_hive_336 as test_hive_336
        ,test_hive_1337 as test_hive_1337
        ,test_hive_1336 as test_hive_1336
        ,test_hive_1335 as test_hive_1335
        ,test_hive_1342 as test_hive_1342
        ,test_hive_1341 as test_hive_1341
        ,test_hive_1340 as test_hive_1340
        ,test_hive_1343 as test_hive_1343
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1349 t1
;

drop view if exists test_hive_1345;

create view test_hive_1345
as
select t1.*
from test_hive_1348 t1
inner join test_hive_1346 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1360 purge;

create table test_hive_1360
(
        test_hive_1354 string
        ,test_hive_1350 string
        ,test_hive_1355 string
        ,test_hive_337 string
        ,test_hive_1353 string
        ,test_hive_1352 string
        ,test_hive_1351 string
        ,test_hive_1358 string
        ,test_hive_1357 string
        ,test_hive_1356 string
        ,test_hive_1359 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1363
(
        test_hive_1354 string
        ,test_hive_1350 string
        ,test_hive_1355 string
        ,test_hive_337 string
        ,test_hive_1353 string
        ,test_hive_1352 string
        ,test_hive_1351 string
        ,test_hive_1358 string
        ,test_hive_1357 string
        ,test_hive_1356 string
        ,test_hive_1359 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1362 purge;

create table if not exists test_hive_1362
(
max_partition bigint
);

drop view if exists test_hive_1365;

create view if not exists test_hive_1365
as
select
        cast(test_hive_1354 as int) as test_hive_1354
        ,cast(test_hive_1350 as int) as test_hive_1350
        ,cast(test_hive_1355 as int) as test_hive_1355
        ,cast(test_hive_337 as string) as test_hive_337
        ,cast(test_hive_1353 as string) as test_hive_1353
        ,cast(from_unixtime(unix_timestamp(test_hive_1352,'yyyymmdd'), 'yyyy-mm-dd') as timestamp) as test_hive_1352
        ,cast(from_unixtime(unix_timestamp(test_hive_1351,'yyyymmdd'), 'yyyy-mm-dd') as timestamp) as test_hive_1351
        ,cast(test_hive_1358 as string) as test_hive_1358
        ,cast(test_hive_1357 as string) as test_hive_1357
        ,cast(test_hive_1356 as string) as test_hive_1356
        ,cast(from_unixtime(unix_timestamp(test_hive_1359,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1359
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1363
;

drop view if exists test_hive_1364;

create view test_hive_1364
as
select
        test_hive_1354 as test_hive_1354
        ,test_hive_1350 as test_hive_1350
        ,test_hive_1355 as test_hive_1355
        ,test_hive_337 as test_hive_337
        ,test_hive_1353 as test_hive_1353
        ,test_hive_1352 as test_hive_1352
        ,test_hive_1351 as test_hive_1351
        ,test_hive_1358 as test_hive_1358
        ,test_hive_1357 as test_hive_1357
        ,test_hive_1356 as test_hive_1356
        ,test_hive_1359 as test_hive_1359
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1365 t1
;

drop view if exists test_hive_1361;

create view test_hive_1361
as
select t1.*
from test_hive_1364 t1
inner join test_hive_1362 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1433 purge;

create table test_hive_1433
(
        test_hive_1427 string
        ,test_hive_1423 string
        ,test_hive_1428 string
        ,test_hive_413 string
        ,test_hive_1426 string
        ,test_hive_1425 string
        ,test_hive_1424 string
        ,test_hive_1431 string
        ,test_hive_1430 string
        ,test_hive_1429 string
        ,test_hive_1432 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1436
(
        test_hive_1427 string
        ,test_hive_1423 string
        ,test_hive_1428 string
        ,test_hive_413 string
        ,test_hive_1426 string
        ,test_hive_1425 string
        ,test_hive_1424 string
        ,test_hive_1431 string
        ,test_hive_1430 string
        ,test_hive_1429 string
        ,test_hive_1432 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1435 purge;

create table if not exists test_hive_1435
(
max_partition bigint
);

drop view if exists test_hive_1438;

create view if not exists test_hive_1438
as
select
        cast(test_hive_1427 as int) as test_hive_1427
        ,cast(test_hive_1423 as int) as test_hive_1423
        ,cast(test_hive_1428 as int) as test_hive_1428
        ,cast(test_hive_413 as decimal) as test_hive_413
        ,cast(test_hive_1426 as string) as test_hive_1426
        ,cast(from_unixtime(unix_timestamp(test_hive_1425,'yyyymmdd'), 'yyyy-mm-dd') as timestamp) as test_hive_1425
        ,cast(from_unixtime(unix_timestamp(test_hive_1424,'yyyymmdd'), 'yyyy-mm-dd') as timestamp) as test_hive_1424
        ,cast(test_hive_1431 as string) as test_hive_1431
        ,cast(test_hive_1430 as string) as test_hive_1430
        ,cast(test_hive_1429 as string) as test_hive_1429
        ,cast(from_unixtime(unix_timestamp(test_hive_1432,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1432
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1436
;

drop view if exists test_hive_1437;

create view test_hive_1437
as
select
        test_hive_1427 as test_hive_1427
        ,test_hive_1423 as test_hive_1423
        ,test_hive_1428 as test_hive_1428
        ,test_hive_413 as test_hive_413
        ,test_hive_1426 as test_hive_1426
        ,test_hive_1425 as test_hive_1425
        ,test_hive_1424 as test_hive_1424
        ,test_hive_1431 as test_hive_1431
        ,test_hive_1430 as test_hive_1430
        ,test_hive_1429 as test_hive_1429
        ,test_hive_1432 as test_hive_1432
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1438 t1
;

drop view if exists test_hive_1434;

create view test_hive_1434
as
select t1.*
from test_hive_1437 t1
inner join test_hive_1435 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1447 purge;

create table test_hive_1447
(
        test_hive_1441 string
        ,test_hive_1439 string
        ,test_hive_1442 string
        ,test_hive_414 string
        ,test_hive_1440 string
        ,test_hive_1445 string
        ,test_hive_1444 string
        ,test_hive_1443 string
        ,test_hive_1446 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1450
(
        test_hive_1441 string
        ,test_hive_1439 string
        ,test_hive_1442 string
        ,test_hive_414 string
        ,test_hive_1440 string
        ,test_hive_1445 string
        ,test_hive_1444 string
        ,test_hive_1443 string
        ,test_hive_1446 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1449 purge;

create table if not exists test_hive_1449
(
max_partition bigint
);

drop view if exists test_hive_1452;

create view if not exists test_hive_1452
as
select
        cast(test_hive_1441 as int) as test_hive_1441
        ,cast(test_hive_1439 as int) as test_hive_1439
        ,cast(test_hive_1442 as int) as test_hive_1442
        ,cast(test_hive_414 as string) as test_hive_414
        ,cast(test_hive_1440 as string) as test_hive_1440
        ,cast(test_hive_1445 as string) as test_hive_1445
        ,cast(test_hive_1444 as string) as test_hive_1444
        ,cast(test_hive_1443 as string) as test_hive_1443
        ,cast(from_unixtime(unix_timestamp(test_hive_1446,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1446
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1450
;

drop view if exists test_hive_1451;

create view test_hive_1451
as
select
        test_hive_1441 as test_hive_1441
        ,test_hive_1439 as test_hive_1439
        ,test_hive_1442 as test_hive_1442
        ,test_hive_414 as test_hive_414
        ,test_hive_1440 as test_hive_1440
        ,test_hive_1445 as test_hive_1445
        ,test_hive_1444 as test_hive_1444
        ,test_hive_1443 as test_hive_1443
        ,test_hive_1446 as test_hive_1446
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1452 t1
;

drop view if exists test_hive_1448;

create view test_hive_1448
as
select t1.*
from test_hive_1451 t1
inner join test_hive_1449 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1463 purge;

create table test_hive_1463
(
        test_hive_1457 string
        ,test_hive_1453 string
        ,test_hive_1458 string
        ,test_hive_415 string
        ,test_hive_1456 string
        ,test_hive_1455 string
        ,test_hive_1454 string
        ,test_hive_1461 string
        ,test_hive_1460 string
        ,test_hive_1459 string
        ,test_hive_1462 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1466
(
        test_hive_1457 string
        ,test_hive_1453 string
        ,test_hive_1458 string
        ,test_hive_415 string
        ,test_hive_1456 string
        ,test_hive_1455 string
        ,test_hive_1454 string
        ,test_hive_1461 string
        ,test_hive_1460 string
        ,test_hive_1459 string
        ,test_hive_1462 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1465 purge;

create table if not exists test_hive_1465
(
max_partition bigint
);

drop view if exists test_hive_1468;

create view if not exists test_hive_1468
as
select
        cast(test_hive_1457 as int) as test_hive_1457
        ,cast(test_hive_1453 as int) as test_hive_1453
        ,cast(test_hive_1458 as int) as test_hive_1458
        ,cast(test_hive_415 as decimal) as test_hive_415
        ,cast(test_hive_1456 as string) as test_hive_1456
        ,cast(from_unixtime(unix_timestamp(test_hive_1455,'yyyymmdd'), 'yyyy-mm-dd') as timestamp) as test_hive_1455
        ,cast(from_unixtime(unix_timestamp(test_hive_1454,'yyyymmdd'), 'yyyy-mm-dd') as timestamp) as test_hive_1454
        ,cast(test_hive_1461 as string) as test_hive_1461
        ,cast(test_hive_1460 as string) as test_hive_1460
        ,cast(test_hive_1459 as string) as test_hive_1459
        ,cast(from_unixtime(unix_timestamp(test_hive_1462,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1462
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1466
;

drop view if exists test_hive_1467;

create view test_hive_1467
as
select
        test_hive_1457 as test_hive_1457
        ,test_hive_1453 as test_hive_1453
        ,test_hive_1458 as test_hive_1458
        ,test_hive_415 as test_hive_415
        ,test_hive_1456 as test_hive_1456
        ,test_hive_1455 as test_hive_1455
        ,test_hive_1454 as test_hive_1454
        ,test_hive_1461 as test_hive_1461
        ,test_hive_1460 as test_hive_1460
        ,test_hive_1459 as test_hive_1459
        ,test_hive_1462 as test_hive_1462
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1468 t1
;

drop view if exists test_hive_1464;

create view test_hive_1464
as
select t1.*
from test_hive_1467 t1
inner join test_hive_1465 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1477 purge;

create table test_hive_1477
(
        test_hive_1471 string
        ,test_hive_1469 string
        ,test_hive_1472 string
        ,test_hive_416 string
        ,test_hive_1470 string
        ,test_hive_1475 string
        ,test_hive_1474 string
        ,test_hive_1473 string
        ,test_hive_1476 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1480
(
        test_hive_1471 string
        ,test_hive_1469 string
        ,test_hive_1472 string
        ,test_hive_416 string
        ,test_hive_1470 string
        ,test_hive_1475 string
        ,test_hive_1474 string
        ,test_hive_1473 string
        ,test_hive_1476 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1479 purge;

create table if not exists test_hive_1479
(
max_partition bigint
);

drop view if exists test_hive_1482;

create view if not exists test_hive_1482
as
select
        cast(test_hive_1471 as int) as test_hive_1471
        ,cast(test_hive_1469 as int) as test_hive_1469
        ,cast(test_hive_1472 as int) as test_hive_1472
        ,cast(test_hive_416 as string) as test_hive_416
        ,cast(test_hive_1470 as string) as test_hive_1470
        ,cast(test_hive_1475 as string) as test_hive_1475
        ,cast(test_hive_1474 as string) as test_hive_1474
        ,cast(test_hive_1473 as string) as test_hive_1473
        ,cast(from_unixtime(unix_timestamp(test_hive_1476,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1476
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1480
;

drop view if exists test_hive_1481;

create view test_hive_1481
as
select
        test_hive_1471 as test_hive_1471
        ,test_hive_1469 as test_hive_1469
        ,test_hive_1472 as test_hive_1472
        ,test_hive_416 as test_hive_416
        ,test_hive_1470 as test_hive_1470
        ,test_hive_1475 as test_hive_1475
        ,test_hive_1474 as test_hive_1474
        ,test_hive_1473 as test_hive_1473
        ,test_hive_1476 as test_hive_1476
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1482 t1
;

drop view if exists test_hive_1478;

create view test_hive_1478
as
select t1.*
from test_hive_1481 t1
inner join test_hive_1479 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1491 purge;

create table test_hive_1491
(
        test_hive_1485 string
        ,test_hive_1483 string
        ,test_hive_1486 string
        ,test_hive_417 string
        ,test_hive_1484 string
        ,test_hive_1489 string
        ,test_hive_1488 string
        ,test_hive_1487 string
        ,test_hive_1490 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1494
(
        test_hive_1485 string
        ,test_hive_1483 string
        ,test_hive_1486 string
        ,test_hive_417 string
        ,test_hive_1484 string
        ,test_hive_1489 string
        ,test_hive_1488 string
        ,test_hive_1487 string
        ,test_hive_1490 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1493 purge;

create table if not exists test_hive_1493
(
max_partition bigint
);

drop view if exists test_hive_1496;

create view if not exists test_hive_1496
as
select
        cast(test_hive_1485 as int) as test_hive_1485
        ,cast(test_hive_1483 as int) as test_hive_1483
        ,cast(test_hive_1486 as int) as test_hive_1486
        ,cast(test_hive_417 as string) as test_hive_417
        ,cast(test_hive_1484 as string) as test_hive_1484
        ,cast(test_hive_1489 as string) as test_hive_1489
        ,cast(test_hive_1488 as string) as test_hive_1488
        ,cast(test_hive_1487 as string) as test_hive_1487
        ,cast(from_unixtime(unix_timestamp(test_hive_1490,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1490
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1494
;

drop view if exists test_hive_1495;

create view test_hive_1495
as
select
        test_hive_1485 as test_hive_1485
        ,test_hive_1483 as test_hive_1483
        ,test_hive_1486 as test_hive_1486
        ,test_hive_417 as test_hive_417
        ,test_hive_1484 as test_hive_1484
        ,test_hive_1489 as test_hive_1489
        ,test_hive_1488 as test_hive_1488
        ,test_hive_1487 as test_hive_1487
        ,test_hive_1490 as test_hive_1490
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1496 t1
;

drop view if exists test_hive_1492;

create view test_hive_1492
as
select t1.*
from test_hive_1495 t1
inner join test_hive_1493 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1507 purge;

create table test_hive_1507
(
        test_hive_1501 string
        ,test_hive_1497 string
        ,test_hive_1502 string
        ,test_hive_418 string
        ,test_hive_1500 string
        ,test_hive_1499 string
        ,test_hive_1498 string
        ,test_hive_1505 string
        ,test_hive_1504 string
        ,test_hive_1503 string
        ,test_hive_1506 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1510
(
        test_hive_1501 string
        ,test_hive_1497 string
        ,test_hive_1502 string
        ,test_hive_418 string
        ,test_hive_1500 string
        ,test_hive_1499 string
        ,test_hive_1498 string
        ,test_hive_1505 string
        ,test_hive_1504 string
        ,test_hive_1503 string
        ,test_hive_1506 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1509 purge;

create table if not exists test_hive_1509
(
max_partition bigint
);

drop view if exists test_hive_1512;

create view if not exists test_hive_1512
as
select
        cast(test_hive_1501 as int) as test_hive_1501
        ,cast(test_hive_1497 as int) as test_hive_1497
        ,cast(test_hive_1502 as int) as test_hive_1502
        ,cast(test_hive_418 as decimal) as test_hive_418
        ,cast(test_hive_1500 as string) as test_hive_1500
        ,cast(from_unixtime(unix_timestamp(test_hive_1499,'yyyymmdd'), 'yyyy-mm-dd') as timestamp) as test_hive_1499
        ,cast(from_unixtime(unix_timestamp(test_hive_1498,'yyyymmdd'), 'yyyy-mm-dd') as timestamp) as test_hive_1498
        ,cast(test_hive_1505 as string) as test_hive_1505
        ,cast(test_hive_1504 as string) as test_hive_1504
        ,cast(test_hive_1503 as string) as test_hive_1503
        ,cast(from_unixtime(unix_timestamp(test_hive_1506,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1506
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1510
;

drop view if exists test_hive_1511;

create view test_hive_1511
as
select
        test_hive_1501 as test_hive_1501
        ,test_hive_1497 as test_hive_1497
        ,test_hive_1502 as test_hive_1502
        ,test_hive_418 as test_hive_418
        ,test_hive_1500 as test_hive_1500
        ,test_hive_1499 as test_hive_1499
        ,test_hive_1498 as test_hive_1498
        ,test_hive_1505 as test_hive_1505
        ,test_hive_1504 as test_hive_1504
        ,test_hive_1503 as test_hive_1503
        ,test_hive_1506 as test_hive_1506
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1512 t1
;

drop view if exists test_hive_1508;

create view test_hive_1508
as
select t1.*
from test_hive_1511 t1
inner join test_hive_1509 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1521 purge;

create table test_hive_1521
(
        test_hive_1515 string
        ,test_hive_1513 string
        ,test_hive_1516 string
        ,test_hive_419 string
        ,test_hive_1514 string
        ,test_hive_1519 string
        ,test_hive_1518 string
        ,test_hive_1517 string
        ,test_hive_1520 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1524
(
        test_hive_1515 string
        ,test_hive_1513 string
        ,test_hive_1516 string
        ,test_hive_419 string
        ,test_hive_1514 string
        ,test_hive_1519 string
        ,test_hive_1518 string
        ,test_hive_1517 string
        ,test_hive_1520 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1523 purge;

create table if not exists test_hive_1523
(
max_partition bigint
);

drop view if exists test_hive_1526;

create view if not exists test_hive_1526
as
select
        cast(test_hive_1515 as int) as test_hive_1515
        ,cast(test_hive_1513 as int) as test_hive_1513
        ,cast(test_hive_1516 as int) as test_hive_1516
        ,cast(test_hive_419 as string) as test_hive_419
        ,cast(test_hive_1514 as string) as test_hive_1514
        ,cast(test_hive_1519 as string) as test_hive_1519
        ,cast(test_hive_1518 as string) as test_hive_1518
        ,cast(test_hive_1517 as string) as test_hive_1517
        ,cast(from_unixtime(unix_timestamp(test_hive_1520,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1520
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1524
;

drop view if exists test_hive_1525;

create view test_hive_1525
as
select
        test_hive_1515 as test_hive_1515
        ,test_hive_1513 as test_hive_1513
        ,test_hive_1516 as test_hive_1516
        ,test_hive_419 as test_hive_419
        ,test_hive_1514 as test_hive_1514
        ,test_hive_1519 as test_hive_1519
        ,test_hive_1518 as test_hive_1518
        ,test_hive_1517 as test_hive_1517
        ,test_hive_1520 as test_hive_1520
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1526 t1
;

drop view if exists test_hive_1522;

create view test_hive_1522
as
select t1.*
from test_hive_1525 t1
inner join test_hive_1523 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1537 purge;

create table test_hive_1537
(
        test_hive_1531 string
        ,test_hive_1527 string
        ,test_hive_1532 string
        ,test_hive_420 string
        ,test_hive_1530 string
        ,test_hive_1529 string
        ,test_hive_1528 string
        ,test_hive_1535 string
        ,test_hive_1534 string
        ,test_hive_1533 string
        ,test_hive_1536 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1540
(
        test_hive_1531 string
        ,test_hive_1527 string
        ,test_hive_1532 string
        ,test_hive_420 string
        ,test_hive_1530 string
        ,test_hive_1529 string
        ,test_hive_1528 string
        ,test_hive_1535 string
        ,test_hive_1534 string
        ,test_hive_1533 string
        ,test_hive_1536 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1539 purge;

create table if not exists test_hive_1539
(
max_partition bigint
);

drop view if exists test_hive_1542;

create view if not exists test_hive_1542
as
select
        cast(test_hive_1531 as int) as test_hive_1531
        ,cast(test_hive_1527 as int) as test_hive_1527
        ,cast(test_hive_1532 as int) as test_hive_1532
        ,cast(test_hive_420 as decimal) as test_hive_420
        ,cast(test_hive_1530 as string) as test_hive_1530
        ,cast(from_unixtime(unix_timestamp(test_hive_1529,'yyyymmdd'), 'yyyy-mm-dd') as timestamp) as test_hive_1529
        ,cast(from_unixtime(unix_timestamp(test_hive_1528,'yyyymmdd'), 'yyyy-mm-dd') as timestamp) as test_hive_1528
        ,cast(test_hive_1535 as string) as test_hive_1535
        ,cast(test_hive_1534 as string) as test_hive_1534
        ,cast(test_hive_1533 as string) as test_hive_1533
        ,cast(from_unixtime(unix_timestamp(test_hive_1536,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1536
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1540
;

drop view if exists test_hive_1541;

create view test_hive_1541
as
select
        test_hive_1531 as test_hive_1531
        ,test_hive_1527 as test_hive_1527
        ,test_hive_1532 as test_hive_1532
        ,test_hive_420 as test_hive_420
        ,test_hive_1530 as test_hive_1530
        ,test_hive_1529 as test_hive_1529
        ,test_hive_1528 as test_hive_1528
        ,test_hive_1535 as test_hive_1535
        ,test_hive_1534 as test_hive_1534
        ,test_hive_1533 as test_hive_1533
        ,test_hive_1536 as test_hive_1536
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1542 t1
;

drop view if exists test_hive_1538;

create view test_hive_1538
as
select t1.*
from test_hive_1541 t1
inner join test_hive_1539 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1849 purge;

create table test_hive_1849
(
        test_hive_1845 string
        ,test_hive_1843 string
        ,test_hive_1846 string
        ,test_hive_445 string
        ,test_hive_1844 string
        ,test_hive_1847 string
        ,test_hive_1848 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1852
(
        test_hive_1845 string
        ,test_hive_1843 string
        ,test_hive_1846 string
        ,test_hive_445 string
        ,test_hive_1844 string
        ,test_hive_1847 string
        ,test_hive_1848 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1851 purge;

create table if not exists test_hive_1851
(
max_partition bigint
);

drop view if exists test_hive_1854;

create view if not exists test_hive_1854
as
select
        cast(test_hive_1845 as int) as test_hive_1845
        ,cast(test_hive_1843 as int) as test_hive_1843
        ,cast(test_hive_1846 as int) as test_hive_1846
        ,cast(test_hive_445 as decimal) as test_hive_445
        ,cast(test_hive_1844 as string) as test_hive_1844
        ,cast(test_hive_1847 as string) as test_hive_1847
        ,cast(from_unixtime(unix_timestamp(test_hive_1848,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1848
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1852
;

drop view if exists test_hive_1853;

create view test_hive_1853
as
select
        test_hive_1845 as test_hive_1845
        ,test_hive_1843 as test_hive_1843
        ,test_hive_1846 as test_hive_1846
        ,test_hive_445 as test_hive_445
        ,test_hive_1844 as test_hive_1844
        ,test_hive_1847 as test_hive_1847
        ,test_hive_1848 as test_hive_1848
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1854 t1
;

drop view if exists test_hive_1850;

create view test_hive_1850
as
select t1.*
from test_hive_1853 t1
inner join test_hive_1851 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1861 purge;

create table test_hive_1861
(
        test_hive_1857 string
        ,test_hive_1855 string
        ,test_hive_1858 string
        ,test_hive_446 string
        ,test_hive_1856 string
        ,test_hive_1859 string
        ,test_hive_1860 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1864
(
        test_hive_1857 string
        ,test_hive_1855 string
        ,test_hive_1858 string
        ,test_hive_446 string
        ,test_hive_1856 string
        ,test_hive_1859 string
        ,test_hive_1860 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1863 purge;

create table if not exists test_hive_1863
(
max_partition bigint
);

drop view if exists test_hive_1866;

create view if not exists test_hive_1866
as
select
        cast(test_hive_1857 as int) as test_hive_1857
        ,cast(test_hive_1855 as int) as test_hive_1855
        ,cast(test_hive_1858 as int) as test_hive_1858
        ,cast(test_hive_446 as string) as test_hive_446
        ,cast(test_hive_1856 as string) as test_hive_1856
        ,cast(test_hive_1859 as string) as test_hive_1859
        ,cast(from_unixtime(unix_timestamp(test_hive_1860,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1860
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1864
;

drop view if exists test_hive_1865;

create view test_hive_1865
as
select
        test_hive_1857 as test_hive_1857
        ,test_hive_1855 as test_hive_1855
        ,test_hive_1858 as test_hive_1858
        ,test_hive_446 as test_hive_446
        ,test_hive_1856 as test_hive_1856
        ,test_hive_1859 as test_hive_1859
        ,test_hive_1860 as test_hive_1860
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1866 t1
;

drop view if exists test_hive_1862;

create view test_hive_1862
as
select t1.*
from test_hive_1865 t1
inner join test_hive_1863 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1873 purge;

create table test_hive_1873
(
        test_hive_1869 string
        ,test_hive_1867 string
        ,test_hive_1870 string
        ,test_hive_447 string
        ,test_hive_1868 string
        ,test_hive_1871 string
        ,test_hive_1872 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1876
(
        test_hive_1869 string
        ,test_hive_1867 string
        ,test_hive_1870 string
        ,test_hive_447 string
        ,test_hive_1868 string
        ,test_hive_1871 string
        ,test_hive_1872 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1875 purge;

create table if not exists test_hive_1875
(
max_partition bigint
);

drop view if exists test_hive_1878;

create view if not exists test_hive_1878
as
select
        cast(test_hive_1869 as int) as test_hive_1869
        ,cast(test_hive_1867 as int) as test_hive_1867
        ,cast(test_hive_1870 as int) as test_hive_1870
        ,cast(test_hive_447 as string) as test_hive_447
        ,cast(test_hive_1868 as string) as test_hive_1868
        ,cast(test_hive_1871 as string) as test_hive_1871
        ,cast(from_unixtime(unix_timestamp(test_hive_1872,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1872
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1876
;

drop view if exists test_hive_1877;

create view test_hive_1877
as
select
        test_hive_1869 as test_hive_1869
        ,test_hive_1867 as test_hive_1867
        ,test_hive_1870 as test_hive_1870
        ,test_hive_447 as test_hive_447
        ,test_hive_1868 as test_hive_1868
        ,test_hive_1871 as test_hive_1871
        ,test_hive_1872 as test_hive_1872
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1878 t1
;

drop view if exists test_hive_1874;

create view test_hive_1874
as
select t1.*
from test_hive_1877 t1
inner join test_hive_1875 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1299 purge;

create table test_hive_1299
(
        test_hive_1288 string
        ,test_hive_1287 string
        ,test_hive_1289 string
        ,test_hive_1282 string
        ,test_hive_1285 string
        ,test_hive_1283 string
        ,test_hive_12832 string
        ,test_hive_1286 string
        ,test_hive_328 string
        ,test_hive_316 string
        ,test_hive_322 string
        ,test_hive_327 string
        ,test_hive_325 string
        ,test_hive_313 string
        ,test_hive_320 string
        ,test_hive_318 string
        ,test_hive_319 string
        ,test_hive_331 string
        ,test_hive_332 string
        ,test_hive_333 string
        ,test_hive_314 string
        ,test_hive_321 string
        ,test_hive_315 string
        ,test_hive_324 string
        ,test_hive_323 string
        ,test_hive_326 string
        ,test_hive_310 string
        ,test_hive_311 string
        ,test_hive_312 string
        ,test_hive_317 string
        ,test_hive_329 string
        ,test_hive_330 string
        ,test_hive_309 string
        ,test_hive_1290 string
        ,test_hive_1290_lag string
        ,test_hive_1290_mil string
        ,test_hive_1290_lag_mil string
        ,test_hive_1290_bp string
        ,test_hive_1290_bp_lag string
        ,test_hive_1290_con string
        ,test_hive_1290_con_lag string
        ,test_hive_1298 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1302
(
        test_hive_1288 string
        ,test_hive_1287 string
        ,test_hive_1289 string
        ,test_hive_1282 string
        ,test_hive_1285 string
        ,test_hive_1283 string
        ,test_hive_12832 string
        ,test_hive_1286 string
        ,test_hive_328 string
        ,test_hive_316 string
        ,test_hive_322 string
        ,test_hive_327 string
        ,test_hive_325 string
        ,test_hive_313 string
        ,test_hive_320 string
        ,test_hive_318 string
        ,test_hive_319 string
        ,test_hive_331 string
        ,test_hive_332 string
        ,test_hive_333 string
        ,test_hive_314 string
        ,test_hive_321 string
        ,test_hive_315 string
        ,test_hive_324 string
        ,test_hive_323 string
        ,test_hive_326 string
        ,test_hive_310 string
        ,test_hive_311 string
        ,test_hive_312 string
        ,test_hive_317 string
        ,test_hive_329 string
        ,test_hive_330 string
        ,test_hive_309 string
        ,test_hive_1290 string
        ,test_hive_1290_lag string
        ,test_hive_1290_mil string
        ,test_hive_1290_lag_mil string
        ,test_hive_1290_bp string
        ,test_hive_1290_bp_lag string
        ,test_hive_1290_con string
        ,test_hive_1290_con_lag string
        ,test_hive_1298 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1301 purge;

create table if not exists test_hive_1301
(
max_partition bigint
);

drop view if exists test_hive_1304;

create view if not exists test_hive_1304
as
select
        cast(test_hive_1288 as int) as test_hive_1288
        ,cast(test_hive_1287 as int) as test_hive_1287
        ,cast(test_hive_1289 as int) as test_hive_1289
        ,cast(test_hive_1282 as string) as test_hive_1282
        ,cast(test_hive_1285 as string) as test_hive_1285
        ,cast(test_hive_1283 as string) as test_hive_1283
        ,cast(test_hive_12832 as string) as test_hive_12832
        ,cast(test_hive_1286 as string) as test_hive_1286
        ,cast(test_hive_328 as string) as test_hive_328
        ,cast(test_hive_316 as string) as test_hive_316
        ,cast(test_hive_322 as string) as test_hive_322
        ,cast(test_hive_327 as string) as test_hive_327
        ,cast(test_hive_325 as string) as test_hive_325
        ,cast(test_hive_313 as string) as test_hive_313
        ,cast(test_hive_320 as string) as test_hive_320
        ,cast(test_hive_318 as string) as test_hive_318
        ,cast(test_hive_319 as string) as test_hive_319
        ,cast(test_hive_331 as string) as test_hive_331
        ,cast(test_hive_332 as string) as test_hive_332
        ,cast(test_hive_333 as string) as test_hive_333
        ,cast(test_hive_314 as string) as test_hive_314
        ,cast(test_hive_321 as string) as test_hive_321
        ,cast(test_hive_315 as string) as test_hive_315
        ,cast(test_hive_324 as string) as test_hive_324
        ,cast(test_hive_323 as string) as test_hive_323
        ,cast(test_hive_326 as string) as test_hive_326
        ,cast(test_hive_310 as string) as test_hive_310
        ,cast(test_hive_311 as string) as test_hive_311
        ,cast(test_hive_312 as string) as test_hive_312
        ,cast(test_hive_317 as string) as test_hive_317
        ,cast(test_hive_329 as string) as test_hive_329
        ,cast(test_hive_330 as string) as test_hive_330
        ,cast(test_hive_309 as string) as test_hive_309
        ,cast(test_hive_1290 as double) as test_hive_1290
        ,cast(test_hive_1290_lag as double) as test_hive_1290_lag
        ,cast(test_hive_1290_mil as double) as test_hive_1290_mil
        ,cast(test_hive_1290_lag_mil as double) as test_hive_1290_lag_mil
        ,cast(test_hive_1290_bp as double) as test_hive_1290_bp
        ,cast(test_hive_1290_bp_lag as double) as test_hive_1290_bp_lag
        ,cast(test_hive_1290_con as double) as test_hive_1290_con
        ,cast(test_hive_1290_con_lag as double) as test_hive_1290_con_lag
        ,cast(from_unixtime(unix_timestamp(test_hive_1298,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1298
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1302
;

drop view if exists test_hive_1303;

create view test_hive_1303
as
select
        test_hive_1288 as test_hive_1288
        ,test_hive_1287 as test_hive_1287
        ,test_hive_1289 as test_hive_1289
        ,test_hive_1282 as test_hive_1282
        ,test_hive_1285 as test_hive_1285
        ,test_hive_1283 as test_hive_1283
        ,test_hive_12832 as test_hive_12832
        ,test_hive_1286 as test_hive_1286
        ,test_hive_328 as test_hive_328
        ,test_hive_316 as test_hive_316
        ,test_hive_322 as test_hive_322
        ,test_hive_327 as test_hive_327
        ,test_hive_325 as test_hive_325
        ,test_hive_313 as test_hive_313
        ,test_hive_320 as test_hive_320
        ,test_hive_318 as test_hive_318
        ,test_hive_319 as test_hive_319
        ,test_hive_331 as test_hive_331
        ,test_hive_332 as test_hive_332
        ,test_hive_333 as test_hive_333
        ,test_hive_314 as test_hive_314
        ,test_hive_321 as test_hive_321
        ,test_hive_315 as test_hive_315
        ,test_hive_324 as test_hive_324
        ,test_hive_323 as test_hive_323
        ,test_hive_326 as test_hive_326
        ,test_hive_310 as test_hive_310
        ,test_hive_311 as test_hive_311
        ,test_hive_312 as test_hive_312
        ,test_hive_317 as test_hive_317
        ,test_hive_329 as test_hive_329
        ,test_hive_330 as test_hive_330
        ,test_hive_309 as test_hive_309
        ,test_hive_1290 as test_hive_1290
        ,test_hive_1290_lag as test_hive_1290_lag
        ,test_hive_1290_mil as test_hive_1290_mil
        ,test_hive_1290_lag_mil as test_hive_1290_lag_mil
        ,test_hive_1290_bp as test_hive_1290_bp
        ,test_hive_1290_bp_lag as test_hive_1290_bp_lag
        ,test_hive_1290_con as test_hive_1290_con
        ,test_hive_1290_con_lag as test_hive_1290_con_lag
        ,test_hive_1298 as test_hive_1298
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1304 t1
;

drop view if exists test_hive_1300;

create view test_hive_1300
as
select t1.*
from test_hive_1303 t1
inner join test_hive_1301 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_2027 purge;

create table test_hive_2027
(
        test_hive_2021 string
        ,test_hive_2019 string
        ,test_hive_2022 string
        ,test_hive_458 string
        ,test_hive_2020 string
        ,test_hive_2025 string
        ,test_hive_2024 string
        ,test_hive_2023 string
        ,test_hive_2026 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_2030
(
        test_hive_2021 string
        ,test_hive_2019 string
        ,test_hive_2022 string
        ,test_hive_458 string
        ,test_hive_2020 string
        ,test_hive_2025 string
        ,test_hive_2024 string
        ,test_hive_2023 string
        ,test_hive_2026 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_2029 purge;

create table if not exists test_hive_2029
(
max_partition bigint
);

drop view if exists test_hive_2032;

create view if not exists test_hive_2032
as
select
        cast(test_hive_2021 as int) as test_hive_2021
        ,cast(test_hive_2019 as int) as test_hive_2019
        ,cast(test_hive_2022 as int) as test_hive_2022
        ,cast(test_hive_458 as string) as test_hive_458
        ,cast(test_hive_2020 as string) as test_hive_2020
        ,cast(test_hive_2025 as string) as test_hive_2025
        ,cast(test_hive_2024 as string) as test_hive_2024
        ,cast(test_hive_2023 as string) as test_hive_2023
        ,cast(from_unixtime(unix_timestamp(test_hive_2026,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_2026
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_2030
;

drop view if exists test_hive_2031;

create view test_hive_2031
as
select
        test_hive_2021 as test_hive_2021
        ,test_hive_2019 as test_hive_2019
        ,test_hive_2022 as test_hive_2022
        ,test_hive_458 as test_hive_458
        ,test_hive_2020 as test_hive_2020
        ,test_hive_2025 as test_hive_2025
        ,test_hive_2024 as test_hive_2024
        ,test_hive_2023 as test_hive_2023
        ,test_hive_2026 as test_hive_2026
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_2032 t1
;

drop view if exists test_hive_2028;

create view test_hive_2028
as
select t1.*
from test_hive_2031 t1
inner join test_hive_2029 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_2013 purge;

create table test_hive_2013
(
        test_hive_2008 string
        ,test_hive_2006 string
        ,test_hive_2009 string
        ,test_hive_457 string
        ,test_hive_2007 string
        ,test_hive_2011 string
        ,test_hive_2010 string
        ,test_hive_2012 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_2016
(
        test_hive_2008 string
        ,test_hive_2006 string
        ,test_hive_2009 string
        ,test_hive_457 string
        ,test_hive_2007 string
        ,test_hive_2011 string
        ,test_hive_2010 string
        ,test_hive_2012 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_2015 purge;

create table if not exists test_hive_2015
(
max_partition bigint
);

drop view if exists test_hive_2018;

create view if not exists test_hive_2018
as
select
        cast(test_hive_2008 as int) as test_hive_2008
        ,cast(test_hive_2006 as int) as test_hive_2006
        ,cast(test_hive_2009 as int) as test_hive_2009
        ,cast(test_hive_457 as string) as test_hive_457
        ,cast(test_hive_2007 as string) as test_hive_2007
        ,cast(test_hive_2011 as string) as test_hive_2011
        ,cast(test_hive_2010 as string) as test_hive_2010
        ,cast(from_unixtime(unix_timestamp(test_hive_2012,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_2012
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_2016
;

drop view if exists test_hive_2017;

create view test_hive_2017
as
select
        test_hive_2008 as test_hive_2008
        ,test_hive_2006 as test_hive_2006
        ,test_hive_2009 as test_hive_2009
        ,test_hive_457 as test_hive_457
        ,test_hive_2007 as test_hive_2007
        ,test_hive_2011 as test_hive_2011
        ,test_hive_2010 as test_hive_2010
        ,test_hive_2012 as test_hive_2012
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_2018 t1
;

drop view if exists test_hive_2014;

create view test_hive_2014
as
select t1.*
from test_hive_2017 t1
inner join test_hive_2015 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_2000 purge;

create table test_hive_2000
(
        test_hive_1996 string
        ,test_hive_1994 string
        ,test_hive_1997 string
        ,test_hive_456 string
        ,test_hive_1995 string
        ,test_hive_1998 string
        ,test_hive_1999 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_2003
(
        test_hive_1996 string
        ,test_hive_1994 string
        ,test_hive_1997 string
        ,test_hive_456 string
        ,test_hive_1995 string
        ,test_hive_1998 string
        ,test_hive_1999 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_2002 purge;

create table if not exists test_hive_2002
(
max_partition bigint
);

drop view if exists test_hive_2005;

create view if not exists test_hive_2005
as
select
        cast(test_hive_1996 as int) as test_hive_1996
        ,cast(test_hive_1994 as int) as test_hive_1994
        ,cast(test_hive_1997 as int) as test_hive_1997
        ,cast(test_hive_456 as string) as test_hive_456
        ,cast(test_hive_1995 as string) as test_hive_1995
        ,cast(test_hive_1998 as string) as test_hive_1998
        ,cast(from_unixtime(unix_timestamp(test_hive_1999,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1999
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_2003
;

drop view if exists test_hive_2004;

create view test_hive_2004
as
select
        test_hive_1996 as test_hive_1996
        ,test_hive_1994 as test_hive_1994
        ,test_hive_1997 as test_hive_1997
        ,test_hive_456 as test_hive_456
        ,test_hive_1995 as test_hive_1995
        ,test_hive_1998 as test_hive_1998
        ,test_hive_1999 as test_hive_1999
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_2005 t1
;

drop view if exists test_hive_2001;

create view test_hive_2001
as
select t1.*
from test_hive_2004 t1
inner join test_hive_2002 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1988 purge;

create table test_hive_1988
(
        test_hive_1984 string
        ,test_hive_1982 string
        ,test_hive_1985 string
        ,test_hive_455 string
        ,test_hive_1983 string
        ,test_hive_1986 string
        ,test_hive_1987 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1991
(
        test_hive_1984 string
        ,test_hive_1982 string
        ,test_hive_1985 string
        ,test_hive_455 string
        ,test_hive_1983 string
        ,test_hive_1986 string
        ,test_hive_1987 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1990 purge;

create table if not exists test_hive_1990
(
max_partition bigint
);

drop view if exists test_hive_1993;

create view if not exists test_hive_1993
as
select
        cast(test_hive_1984 as int) as test_hive_1984
        ,cast(test_hive_1982 as int) as test_hive_1982
        ,cast(test_hive_1985 as int) as test_hive_1985
        ,cast(test_hive_455 as string) as test_hive_455
        ,cast(test_hive_1983 as string) as test_hive_1983
        ,cast(test_hive_1986 as string) as test_hive_1986
        ,cast(from_unixtime(unix_timestamp(test_hive_1987,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1987
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1991
;

drop view if exists test_hive_1992;

create view test_hive_1992
as
select
        test_hive_1984 as test_hive_1984
        ,test_hive_1982 as test_hive_1982
        ,test_hive_1985 as test_hive_1985
        ,test_hive_455 as test_hive_455
        ,test_hive_1983 as test_hive_1983
        ,test_hive_1986 as test_hive_1986
        ,test_hive_1987 as test_hive_1987
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
        ,ds
from test_hive_1993 t1
;

drop view if exists test_hive_1989;

create view test_hive_1989
as
select t1.*
from test_hive_1992 t1
inner join test_hive_1990 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1976 purge;

create table test_hive_1976
(
        test_hive_1970 string
        ,test_hive_1968 string
        ,test_hive_1971 string
        ,test_hive_454 string
        ,test_hive_1969 string
        ,test_hive_1974 string
        ,test_hive_1973 string
        ,test_hive_1972 string
        ,test_hive_1975 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1979
(
        test_hive_1970 string
        ,test_hive_1968 string
        ,test_hive_1971 string
        ,test_hive_454 string
        ,test_hive_1969 string
        ,test_hive_1974 string
        ,test_hive_1973 string
        ,test_hive_1972 string
        ,test_hive_1975 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1978 purge;

create table if not exists test_hive_1978
(
max_partition bigint
);

drop view if exists test_hive_1981;

create view if not exists test_hive_1981
as
select
        cast(test_hive_1970 as int) as test_hive_1970
        ,cast(test_hive_1968 as int) as test_hive_1968
        ,cast(test_hive_1971 as int) as test_hive_1971
        ,cast(test_hive_454 as string) as test_hive_454
        ,cast(test_hive_1969 as string) as test_hive_1969
        ,cast(test_hive_1974 as string) as test_hive_1974
        ,cast(test_hive_1973 as string) as test_hive_1973
        ,cast(test_hive_1972 as string) as test_hive_1972
        ,cast(from_unixtime(unix_timestamp(test_hive_1975,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1975
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1979
;

drop view if exists test_hive_1980;

create view test_hive_1980
as
select
        test_hive_1970 as test_hive_1970
        ,test_hive_1968 as test_hive_1968
        ,test_hive_1971 as test_hive_1971
        ,test_hive_454 as test_hive_454
        ,test_hive_1969 as test_hive_1969
        ,test_hive_1974 as test_hive_1974
        ,test_hive_1973 as test_hive_1973
        ,test_hive_1972 as test_hive_1972
        ,test_hive_1975 as test_hive_1975
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1981 t1
;

drop view if exists test_hive_1977;

create view test_hive_1977
as
select t1.*
from test_hive_1980 t1
inner join test_hive_1978 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1962 purge;

create table test_hive_1962
(
        test_hive_1958 string
        ,test_hive_1956 string
        ,test_hive_1959 string
        ,test_hive_453 string
        ,test_hive_1957 string
        ,test_hive_1960 string
        ,test_hive_1961 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1965
(
        test_hive_1958 string
        ,test_hive_1956 string
        ,test_hive_1959 string
        ,test_hive_453 string
        ,test_hive_1957 string
        ,test_hive_1960 string
        ,test_hive_1961 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1964 purge;

create table if not exists test_hive_1964
(
max_partition bigint
);

drop view if exists test_hive_1967;

create view if not exists test_hive_1967
as
select
        cast(test_hive_1958 as int) as test_hive_1958
        ,cast(test_hive_1956 as int) as test_hive_1956
        ,cast(test_hive_1959 as int) as test_hive_1959
        ,cast(test_hive_453 as string) as test_hive_453
        ,cast(test_hive_1957 as string) as test_hive_1957
        ,cast(test_hive_1960 as string) as test_hive_1960
        ,cast(from_unixtime(unix_timestamp(test_hive_1961,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1961
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1965
;

drop view if exists test_hive_1966;

create view test_hive_1966
as
select
        test_hive_1958 as test_hive_1958
        ,test_hive_1956 as test_hive_1956
        ,test_hive_1959 as test_hive_1959
        ,test_hive_453 as test_hive_453
        ,test_hive_1957 as test_hive_1957
        ,test_hive_1960 as test_hive_1960
        ,test_hive_1961 as test_hive_1961
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1967 t1
;

drop view if exists test_hive_1963;

create view test_hive_1963
as
select t1.*
from test_hive_1966 t1
inner join test_hive_1964 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1950 purge;

create table test_hive_1950
(
        test_hive_1946 string
        ,test_hive_1944 string
        ,test_hive_1947 string
        ,test_hive_452 string
        ,test_hive_1945 string
        ,test_hive_1948 string
        ,test_hive_1949 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1953
(
        test_hive_1946 string
        ,test_hive_1944 string
        ,test_hive_1947 string
        ,test_hive_452 string
        ,test_hive_1945 string
        ,test_hive_1948 string
        ,test_hive_1949 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1952 purge;

create table if not exists test_hive_1952
(
max_partition bigint
);

drop view if exists test_hive_1955;

create view if not exists test_hive_1955
as
select
        cast(test_hive_1946 as int) as test_hive_1946
        ,cast(test_hive_1944 as int) as test_hive_1944
        ,cast(test_hive_1947 as int) as test_hive_1947
        ,cast(test_hive_452 as string) as test_hive_452
        ,cast(test_hive_1945 as string) as test_hive_1945
        ,cast(test_hive_1948 as string) as test_hive_1948
        ,cast(from_unixtime(unix_timestamp(test_hive_1949,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1949
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1953
;

drop view if exists test_hive_1954;

create view test_hive_1954
as
select
        test_hive_1946 as test_hive_1946
        ,test_hive_1944 as test_hive_1944
        ,test_hive_1947 as test_hive_1947
        ,test_hive_452 as test_hive_452
        ,test_hive_1945 as test_hive_1945
        ,test_hive_1948 as test_hive_1948
        ,test_hive_1949 as test_hive_1949
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
        ,ds
from test_hive_1955 t1
;

drop view if exists test_hive_1951;

create view test_hive_1951
as
select t1.*
from test_hive_1954 t1
inner join test_hive_1952 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1938 purge;

create table test_hive_1938
(
        test_hive_1933 string
        ,test_hive_1931 string
        ,test_hive_1934 string
        ,test_hive_451 string
        ,test_hive_1932 string
        ,test_hive_1936 string
        ,test_hive_1935 string
        ,test_hive_1937 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1941
(
        test_hive_1933 string
        ,test_hive_1931 string
        ,test_hive_1934 string
        ,test_hive_451 string
        ,test_hive_1932 string
        ,test_hive_1936 string
        ,test_hive_1935 string
        ,test_hive_1937 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1940 purge;

create table if not exists test_hive_1940
(
max_partition bigint
);

drop view if exists test_hive_1943;

create view if not exists test_hive_1943
as
select
        cast(test_hive_1933 as int) as test_hive_1933
        ,cast(test_hive_1931 as int) as test_hive_1931
        ,cast(test_hive_1934 as int) as test_hive_1934
        ,cast(test_hive_451 as string) as test_hive_451
        ,cast(test_hive_1932 as string) as test_hive_1932
        ,cast(test_hive_1936 as string) as test_hive_1936
        ,cast(test_hive_1935 as string) as test_hive_1935
        ,cast(from_unixtime(unix_timestamp(test_hive_1937,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1937
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1941
;

drop view if exists test_hive_1942;

create view test_hive_1942
as
select
        test_hive_1933 as test_hive_1933
        ,test_hive_1931 as test_hive_1931
        ,test_hive_1934 as test_hive_1934
        ,test_hive_451 as test_hive_451
        ,test_hive_1932 as test_hive_1932
        ,test_hive_1936 as test_hive_1936
        ,test_hive_1935 as test_hive_1935
        ,test_hive_1937 as test_hive_1937
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1943 t1
;

drop view if exists test_hive_1939;

create view test_hive_1939
as
select t1.*
from test_hive_1942 t1
inner join test_hive_1940 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1925 purge;

create table test_hive_1925
(
        test_hive_1919 string
        ,test_hive_1916 string
        ,test_hive_1920 string
        ,test_hive_1918 string
        ,test_hive_1917 string
        ,test_hive_1923 string
        ,test_hive_1922 string
        ,test_hive_1921 string
        ,test_hive_1924 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1928
(
        test_hive_1919 string
        ,test_hive_1916 string
        ,test_hive_1920 string
        ,test_hive_1918 string
        ,test_hive_1917 string
        ,test_hive_1923 string
        ,test_hive_1922 string
        ,test_hive_1921 string
        ,test_hive_1924 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1927 purge;

create table if not exists test_hive_1927
(
max_partition bigint
);

drop view if exists test_hive_1930;

create view if not exists test_hive_1930
as
select
        cast(test_hive_1919 as int) as test_hive_1919
        ,cast(test_hive_1916 as int) as test_hive_1916
        ,cast(test_hive_1920 as int) as test_hive_1920
        ,cast(test_hive_1918 as string) as test_hive_1918
        ,cast(test_hive_1917 as string) as test_hive_1917
        ,cast(test_hive_1923 as string) as test_hive_1923
        ,cast(test_hive_1922 as string) as test_hive_1922
        ,cast(test_hive_1921 as string) as test_hive_1921
        ,cast(from_unixtime(unix_timestamp(test_hive_1924,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1924
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1928
;

drop view if exists test_hive_1929;

create view test_hive_1929
as
select
        test_hive_1919 as test_hive_1919
        ,test_hive_1916 as test_hive_1916
        ,test_hive_1920 as test_hive_1920
        ,test_hive_1918 as test_hive_1918
        ,test_hive_1917 as test_hive_1917
        ,test_hive_1923 as test_hive_1923
        ,test_hive_1922 as test_hive_1922
        ,test_hive_1921 as test_hive_1921
        ,test_hive_1924 as test_hive_1924
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
        ,ds
from test_hive_1930 t1
;

drop view if exists test_hive_1926;

create view test_hive_1926
as
select t1.*
from test_hive_1929 t1
inner join test_hive_1927 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1910 purge;

create table test_hive_1910
(
        test_hive_1905 string
        ,test_hive_1903 string
        ,test_hive_1906 string
        ,test_hive_450 string
        ,test_hive_1904 string
        ,test_hive_1908 string
        ,test_hive_1907 string
        ,test_hive_1909 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1913
(
        test_hive_1905 string
        ,test_hive_1903 string
        ,test_hive_1906 string
        ,test_hive_450 string
        ,test_hive_1904 string
        ,test_hive_1908 string
        ,test_hive_1907 string
        ,test_hive_1909 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1912 purge;

create table if not exists test_hive_1912
(
max_partition bigint
);

drop view if exists test_hive_1915;

create view if not exists test_hive_1915
as
select
        cast(test_hive_1905 as int) as test_hive_1905
        ,cast(test_hive_1903 as int) as test_hive_1903
        ,cast(test_hive_1906 as int) as test_hive_1906
        ,cast(test_hive_450 as string) as test_hive_450
        ,cast(test_hive_1904 as string) as test_hive_1904
        ,cast(test_hive_1908 as string) as test_hive_1908
        ,cast(test_hive_1907 as string) as test_hive_1907
        ,cast(from_unixtime(unix_timestamp(test_hive_1909,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1909
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1913
;

drop view if exists test_hive_1914;

create view test_hive_1914
as
select
        test_hive_1905 as test_hive_1905
        ,test_hive_1903 as test_hive_1903
        ,test_hive_1906 as test_hive_1906
        ,test_hive_450 as test_hive_450
        ,test_hive_1904 as test_hive_1904
        ,test_hive_1908 as test_hive_1908
        ,test_hive_1907 as test_hive_1907
        ,test_hive_1909 as test_hive_1909
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
        ,ds
from test_hive_1915 t1
;

drop view if exists test_hive_1911;

create view test_hive_1911
as
select t1.*
from test_hive_1914 t1
inner join test_hive_1912 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1897 purge;

create table test_hive_1897
(
        test_hive_1893 string
        ,test_hive_1891 string
        ,test_hive_1894 string
        ,test_hive_449 string
        ,test_hive_1892 string
        ,test_hive_1895 string
        ,test_hive_1896 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1900
(
        test_hive_1893 string
        ,test_hive_1891 string
        ,test_hive_1894 string
        ,test_hive_449 string
        ,test_hive_1892 string
        ,test_hive_1895 string
        ,test_hive_1896 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1899 purge;

create table if not exists test_hive_1899
(
max_partition bigint
);

drop view if exists test_hive_1902;

create view if not exists test_hive_1902
as
select
        cast(test_hive_1893 as int) as test_hive_1893
        ,cast(test_hive_1891 as int) as test_hive_1891
        ,cast(test_hive_1894 as int) as test_hive_1894
        ,cast(test_hive_449 as string) as test_hive_449
        ,cast(test_hive_1892 as string) as test_hive_1892
        ,cast(test_hive_1895 as string) as test_hive_1895
        ,cast(from_unixtime(unix_timestamp(test_hive_1896,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1896
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1900
;

drop view if exists test_hive_1901;

create view test_hive_1901
as
select
        test_hive_1893 as test_hive_1893
        ,test_hive_1891 as test_hive_1891
        ,test_hive_1894 as test_hive_1894
        ,test_hive_449 as test_hive_449
        ,test_hive_1892 as test_hive_1892
        ,test_hive_1895 as test_hive_1895
        ,test_hive_1896 as test_hive_1896
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
        ,ds
from test_hive_1902 t1
;

drop view if exists test_hive_1898;

create view test_hive_1898
as
select t1.*
from test_hive_1901 t1
inner join test_hive_1899 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1885 purge;

create table test_hive_1885
(
        test_hive_1881 string
        ,test_hive_1879 string
        ,test_hive_1882 string
        ,test_hive_448 string
        ,test_hive_1880 string
        ,test_hive_1883 string
        ,test_hive_1884 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1888
(
        test_hive_1881 string
        ,test_hive_1879 string
        ,test_hive_1882 string
        ,test_hive_448 string
        ,test_hive_1880 string
        ,test_hive_1883 string
        ,test_hive_1884 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1887 purge;

create table if not exists test_hive_1887
(
max_partition bigint
);

drop view if exists test_hive_1890;

create view if not exists test_hive_1890
as
select
        cast(test_hive_1881 as int) as test_hive_1881
        ,cast(test_hive_1879 as int) as test_hive_1879
        ,cast(test_hive_1882 as int) as test_hive_1882
        ,cast(test_hive_448 as string) as test_hive_448
        ,cast(test_hive_1880 as string) as test_hive_1880
        ,cast(test_hive_1883 as string) as test_hive_1883
        ,cast(from_unixtime(unix_timestamp(test_hive_1884,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1884
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1888
;

drop view if exists test_hive_1889;

create view test_hive_1889
as
select
        test_hive_1881 as test_hive_1881
        ,test_hive_1879 as test_hive_1879
        ,test_hive_1882 as test_hive_1882
        ,test_hive_448 as test_hive_448
        ,test_hive_1880 as test_hive_1880
        ,test_hive_1883 as test_hive_1883
        ,test_hive_1884 as test_hive_1884
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
        ,ds
from test_hive_1890 t1
;

drop view if exists test_hive_1886;

create view test_hive_1886
as
select t1.*
from test_hive_1889 t1
inner join test_hive_1887 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1837 purge;

create table test_hive_1837
(
        test_hive_1832 string
        ,test_hive_1830 string
        ,test_hive_1833 string
        ,test_hive_444 string
        ,test_hive_1831 string
        ,test_hive_1835 string
        ,test_hive_1834 string
        ,test_hive_1836 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1840
(
        test_hive_1832 string
        ,test_hive_1830 string
        ,test_hive_1833 string
        ,test_hive_444 string
        ,test_hive_1831 string
        ,test_hive_1835 string
        ,test_hive_1834 string
        ,test_hive_1836 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1839 purge;

create table if not exists test_hive_1839
(
max_partition bigint
);

drop view if exists test_hive_1842;

create view if not exists test_hive_1842
as
select
        cast(test_hive_1832 as int) as test_hive_1832
        ,cast(test_hive_1830 as int) as test_hive_1830
        ,cast(test_hive_1833 as int) as test_hive_1833
        ,cast(test_hive_444 as string) as test_hive_444
        ,cast(test_hive_1831 as string) as test_hive_1831
        ,cast(test_hive_1835 as string) as test_hive_1835
        ,cast(test_hive_1834 as string) as test_hive_1834
        ,cast(from_unixtime(unix_timestamp(test_hive_1836,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1836
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1840
;

drop view if exists test_hive_1841;

create view test_hive_1841
as
select
        test_hive_1832 as test_hive_1832
        ,test_hive_1830 as test_hive_1830
        ,test_hive_1833 as test_hive_1833
        ,test_hive_444 as test_hive_444
        ,test_hive_1831 as test_hive_1831
        ,test_hive_1835 as test_hive_1835
        ,test_hive_1834 as test_hive_1834
        ,test_hive_1836 as test_hive_1836
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
        ,ds
from test_hive_1842 t1
;

drop view if exists test_hive_1838;

create view test_hive_1838
as
select t1.*
from test_hive_1841 t1
inner join test_hive_1839 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1824 purge;

create table test_hive_1824
(
        test_hive_1818 string
        ,test_hive_1816 string
        ,test_hive_1819 string
        ,test_hive_443 string
        ,test_hive_1817 string
        ,test_hive_1822 string
        ,test_hive_1821 string
        ,test_hive_1820 string
        ,test_hive_1823 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1827
(
        test_hive_1818 string
        ,test_hive_1816 string
        ,test_hive_1819 string
        ,test_hive_443 string
        ,test_hive_1817 string
        ,test_hive_1822 string
        ,test_hive_1821 string
        ,test_hive_1820 string
        ,test_hive_1823 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1826 purge;

create table if not exists test_hive_1826
(
max_partition bigint
);

drop view if exists test_hive_1829;

create view if not exists test_hive_1829
as
select
        cast(test_hive_1818 as int) as test_hive_1818
        ,cast(test_hive_1816 as int) as test_hive_1816
        ,cast(test_hive_1819 as int) as test_hive_1819
        ,cast(test_hive_443 as string) as test_hive_443
        ,cast(test_hive_1817 as string) as test_hive_1817
        ,cast(test_hive_1822 as string) as test_hive_1822
        ,cast(test_hive_1821 as string) as test_hive_1821
        ,cast(test_hive_1820 as string) as test_hive_1820
        ,cast(from_unixtime(unix_timestamp(test_hive_1823,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1823
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1827
;

drop view if exists test_hive_1828;

create view test_hive_1828
as
select
        test_hive_1818 as test_hive_1818
        ,test_hive_1816 as test_hive_1816
        ,test_hive_1819 as test_hive_1819
        ,test_hive_443 as test_hive_443
        ,test_hive_1817 as test_hive_1817
        ,test_hive_1822 as test_hive_1822
        ,test_hive_1821 as test_hive_1821
        ,test_hive_1820 as test_hive_1820
        ,test_hive_1823 as test_hive_1823
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
        ,ds
from test_hive_1829 t1
;

drop view if exists test_hive_1825;

create view test_hive_1825
as
select t1.*
from test_hive_1828 t1
inner join test_hive_1826 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1810 purge;

create table test_hive_1810
(
        test_hive_1804 string
        ,test_hive_1802 string
        ,test_hive_1805 string
        ,test_hive_442 string
        ,test_hive_1803 string
        ,test_hive_1808 string
        ,test_hive_1807 string
        ,test_hive_1806 string
        ,test_hive_1809 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1813
(
        test_hive_1804 string
        ,test_hive_1802 string
        ,test_hive_1805 string
        ,test_hive_442 string
        ,test_hive_1803 string
        ,test_hive_1808 string
        ,test_hive_1807 string
        ,test_hive_1806 string
        ,test_hive_1809 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1812 purge;

create table if not exists test_hive_1812
(
max_partition bigint
);

drop view if exists test_hive_1815;

create view if not exists test_hive_1815
as
select
        cast(test_hive_1804 as int) as test_hive_1804
        ,cast(test_hive_1802 as int) as test_hive_1802
        ,cast(test_hive_1805 as int) as test_hive_1805
        ,cast(test_hive_442 as string) as test_hive_442
        ,cast(test_hive_1803 as string) as test_hive_1803
        ,cast(test_hive_1808 as string) as test_hive_1808
        ,cast(test_hive_1807 as string) as test_hive_1807
        ,cast(test_hive_1806 as string) as test_hive_1806
        ,cast(from_unixtime(unix_timestamp(test_hive_1809,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1809
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1813
;

drop view if exists test_hive_1814;

create view test_hive_1814
as
select
        test_hive_1804 as test_hive_1804
        ,test_hive_1802 as test_hive_1802
        ,test_hive_1805 as test_hive_1805
        ,test_hive_442 as test_hive_442
        ,test_hive_1803 as test_hive_1803
        ,test_hive_1808 as test_hive_1808
        ,test_hive_1807 as test_hive_1807
        ,test_hive_1806 as test_hive_1806
        ,test_hive_1809 as test_hive_1809
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
        ,ds
from test_hive_1815 t1
;

drop view if exists test_hive_1811;

create view test_hive_1811
as
select t1.*
from test_hive_1814 t1
inner join test_hive_1812 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1796 purge;

create table test_hive_1796
(
        test_hive_1790 string
        ,test_hive_1788 string
        ,test_hive_1791 string
        ,test_hive_441 string
        ,test_hive_1789 string
        ,test_hive_1794 string
        ,test_hive_1793 string
        ,test_hive_1792 string
        ,test_hive_1795 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1799
(
        test_hive_1790 string
        ,test_hive_1788 string
        ,test_hive_1791 string
        ,test_hive_441 string
        ,test_hive_1789 string
        ,test_hive_1794 string
        ,test_hive_1793 string
        ,test_hive_1792 string
        ,test_hive_1795 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1798 purge;

create table if not exists test_hive_1798
(
max_partition bigint
);

drop view if exists test_hive_1801;

create view if not exists test_hive_1801
as
select
        cast(test_hive_1790 as int) as test_hive_1790
        ,cast(test_hive_1788 as int) as test_hive_1788
        ,cast(test_hive_1791 as int) as test_hive_1791
        ,cast(test_hive_441 as string) as test_hive_441
        ,cast(test_hive_1789 as string) as test_hive_1789
        ,cast(test_hive_1794 as string) as test_hive_1794
        ,cast(test_hive_1793 as string) as test_hive_1793
        ,cast(test_hive_1792 as string) as test_hive_1792
        ,cast(from_unixtime(unix_timestamp(test_hive_1795,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1795
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1799
;

drop view if exists test_hive_1800;

create view test_hive_1800
as
select
        test_hive_1790 as test_hive_1790
        ,test_hive_1788 as test_hive_1788
        ,test_hive_1791 as test_hive_1791
        ,test_hive_441 as test_hive_441
        ,test_hive_1789 as test_hive_1789
        ,test_hive_1794 as test_hive_1794
        ,test_hive_1793 as test_hive_1793
        ,test_hive_1792 as test_hive_1792
        ,test_hive_1795 as test_hive_1795
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
        ,ds
from test_hive_1801 t1
;

drop view if exists test_hive_1797;

create view test_hive_1797
as
select t1.*
from test_hive_1800 t1
inner join test_hive_1798 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1782 purge;

create table test_hive_1782
(
        test_hive_1776 string
        ,test_hive_1774 string
        ,test_hive_1777 string
        ,test_hive_440 string
        ,test_hive_1775 string
        ,test_hive_1780 string
        ,test_hive_1779 string
        ,test_hive_1778 string
        ,test_hive_1781 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1785
(
        test_hive_1776 string
        ,test_hive_1774 string
        ,test_hive_1777 string
        ,test_hive_440 string
        ,test_hive_1775 string
        ,test_hive_1780 string
        ,test_hive_1779 string
        ,test_hive_1778 string
        ,test_hive_1781 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1784 purge;

create table if not exists test_hive_1784
(
max_partition bigint
);

drop view if exists test_hive_1787;

create view if not exists test_hive_1787
as
select
        cast(test_hive_1776 as int) as test_hive_1776
        ,cast(test_hive_1774 as int) as test_hive_1774
        ,cast(test_hive_1777 as int) as test_hive_1777
        ,cast(test_hive_440 as string) as test_hive_440
        ,cast(test_hive_1775 as string) as test_hive_1775
        ,cast(test_hive_1780 as string) as test_hive_1780
        ,cast(test_hive_1779 as string) as test_hive_1779
        ,cast(test_hive_1778 as string) as test_hive_1778
        ,cast(from_unixtime(unix_timestamp(test_hive_1781,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1781
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1785
;

drop view if exists test_hive_1786;

create view test_hive_1786
as
select
        test_hive_1776 as test_hive_1776
        ,test_hive_1774 as test_hive_1774
        ,test_hive_1777 as test_hive_1777
        ,test_hive_440 as test_hive_440
        ,test_hive_1775 as test_hive_1775
        ,test_hive_1780 as test_hive_1780
        ,test_hive_1779 as test_hive_1779
        ,test_hive_1778 as test_hive_1778
        ,test_hive_1781 as test_hive_1781
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1787 t1
;

drop view if exists test_hive_1783;

create view test_hive_1783
as
select t1.*
from test_hive_1786 t1
inner join test_hive_1784 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1768 purge;

create table test_hive_1768
(
        test_hive_1764 string
        ,test_hive_1762 string
        ,test_hive_1765 string
        ,test_hive_438 string
        ,test_hive_439 string
        ,test_hive_1763 string
        ,test_hive_1766 string
        ,test_hive_1767 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1771
(
        test_hive_1764 string
        ,test_hive_1762 string
        ,test_hive_1765 string
        ,test_hive_438 string
        ,test_hive_439 string
        ,test_hive_1763 string
        ,test_hive_1766 string
        ,test_hive_1767 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1770 purge;

create table if not exists test_hive_1770
(
max_partition bigint
);

drop view if exists test_hive_1773;

create view if not exists test_hive_1773
as
select
        cast(test_hive_1764 as int) as test_hive_1764
        ,cast(test_hive_1762 as int) as test_hive_1762
        ,cast(test_hive_1765 as int) as test_hive_1765
        ,cast(test_hive_438 as string) as test_hive_438
        ,cast(test_hive_439 as string) as test_hive_439
        ,cast(test_hive_1763 as string) as test_hive_1763
        ,cast(test_hive_1766 as string) as test_hive_1766
        ,cast(from_unixtime(unix_timestamp(test_hive_1767,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1767
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1771
;

drop view if exists test_hive_1772;

create view test_hive_1772
as
select
        test_hive_1764 as test_hive_1764
        ,test_hive_1762 as test_hive_1762
        ,test_hive_1765 as test_hive_1765
        ,test_hive_438 as test_hive_438
        ,test_hive_439 as test_hive_439
        ,test_hive_1763 as test_hive_1763
        ,test_hive_1766 as test_hive_1766
        ,test_hive_1767 as test_hive_1767
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1773 t1
;

drop view if exists test_hive_1769;

create view test_hive_1769
as
select t1.*
from test_hive_1772 t1
inner join test_hive_1770 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1756 purge;

create table test_hive_1756
(
        test_hive_1752 string
        ,test_hive_1750 string
        ,test_hive_1753 string
        ,test_hive_437 string
        ,test_hive_1751 string
        ,test_hive_1754 string
        ,test_hive_1755 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1759
(
        test_hive_1752 string
        ,test_hive_1750 string
        ,test_hive_1753 string
        ,test_hive_437 string
        ,test_hive_1751 string
        ,test_hive_1754 string
        ,test_hive_1755 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1758 purge;

create table if not exists test_hive_1758
(
max_partition bigint
);

drop view if exists test_hive_1761;

create view if not exists test_hive_1761
as
select
        cast(test_hive_1752 as int) as test_hive_1752
        ,cast(test_hive_1750 as int) as test_hive_1750
        ,cast(test_hive_1753 as int) as test_hive_1753
        ,cast(test_hive_437 as string) as test_hive_437
        ,cast(test_hive_1751 as string) as test_hive_1751
        ,cast(test_hive_1754 as string) as test_hive_1754
        ,cast(from_unixtime(unix_timestamp(test_hive_1755,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1755
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1759
;

drop view if exists test_hive_1760;

create view test_hive_1760
as
select
        test_hive_1752 as test_hive_1752
        ,test_hive_1750 as test_hive_1750
        ,test_hive_1753 as test_hive_1753
        ,test_hive_437 as test_hive_437
        ,test_hive_1751 as test_hive_1751
        ,test_hive_1754 as test_hive_1754
        ,test_hive_1755 as test_hive_1755
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1761 t1
;

drop view if exists test_hive_1757;

create view test_hive_1757
as
select t1.*
from test_hive_1760 t1
inner join test_hive_1758 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1744 purge;

create table test_hive_1744
(
        test_hive_1740 string
        ,test_hive_1738 string
        ,test_hive_1741 string
        ,test_hive_436 string
        ,test_hive_1739 string
        ,test_hive_1742 string
        ,test_hive_1743 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1747
(
        test_hive_1740 string
        ,test_hive_1738 string
        ,test_hive_1741 string
        ,test_hive_436 string
        ,test_hive_1739 string
        ,test_hive_1742 string
        ,test_hive_1743 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1746 purge;

create table if not exists test_hive_1746
(
max_partition bigint
);

drop view if exists test_hive_1749;

create view if not exists test_hive_1749
as
select
        cast(test_hive_1740 as int) as test_hive_1740
        ,cast(test_hive_1738 as int) as test_hive_1738
        ,cast(test_hive_1741 as int) as test_hive_1741
        ,cast(test_hive_436 as string) as test_hive_436
        ,cast(test_hive_1739 as string) as test_hive_1739
        ,cast(test_hive_1742 as string) as test_hive_1742
        ,cast(from_unixtime(unix_timestamp(test_hive_1743,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1743
        ,source_file_name
        ,cast(creation_date as timestamp) as creation_date
        ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1747
;

drop view if exists test_hive_1748;

create view test_hive_1748
as
select
        test_hive_1740 as test_hive_1740
        ,test_hive_1738 as test_hive_1738
        ,test_hive_1741 as test_hive_1741
        ,test_hive_436 as test_hive_436
        ,test_hive_1739 as test_hive_1739
        ,test_hive_1742 as test_hive_1742
        ,test_hive_1743 as test_hive_1743
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1749 t1
;

drop view if exists test_hive_1745;

create view test_hive_1745
as
select t1.*
from test_hive_1748 t1
inner join test_hive_1746 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1732 purge;

create table test_hive_1732
(
        test_hive_1728 string
        ,test_hive_1726 string
        ,test_hive_1729 string
        ,test_hive_435 string
        ,test_hive_1727 string
        ,test_hive_1730 string
        ,test_hive_1731 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1735
(
        test_hive_1728 string
        ,test_hive_1726 string
        ,test_hive_1729 string
        ,test_hive_435 string
        ,test_hive_1727 string
        ,test_hive_1730 string
        ,test_hive_1731 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1734 purge;

create table if not exists test_hive_1734
(
max_partition bigint
);

drop view if exists test_hive_1737;

create view if not exists test_hive_1737
as
select
        cast(test_hive_1728 as int) as test_hive_1728
        ,cast(test_hive_1726 as int) as test_hive_1726
        ,cast(test_hive_1729 as int) as test_hive_1729
        ,cast(test_hive_435 as string) as test_hive_435
        ,cast(test_hive_1727 as string) as test_hive_1727
        ,cast(test_hive_1730 as string) as test_hive_1730
        ,cast(from_unixtime(unix_timestamp(test_hive_1731,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1731
       ,source_file_name
       ,cast(creation_date as timestamp) as creation_date
       ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1735
;

drop view if exists test_hive_1736;

create view test_hive_1736
as
select
        test_hive_1728 as test_hive_1728
        ,test_hive_1726 as test_hive_1726
        ,test_hive_1729 as test_hive_1729
        ,test_hive_435 as test_hive_435
        ,test_hive_1727 as test_hive_1727
        ,test_hive_1730 as test_hive_1730
        ,test_hive_1731 as test_hive_1731
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1737 t1
;

drop view if exists test_hive_1733;

create view test_hive_1733
as
select t1.*
from test_hive_1736 t1
inner join test_hive_1734 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1720 purge;

create table test_hive_1720
(
        test_hive_1714 string
        ,test_hive_1712 string
        ,test_hive_1715 string
        ,test_hive_434 string
        ,test_hive_1713 string
        ,test_hive_1718 string
        ,test_hive_1717 string
        ,test_hive_1716 string
        ,test_hive_1719 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1723
(
        test_hive_1714 string
        ,test_hive_1712 string
        ,test_hive_1715 string
        ,test_hive_434 string
        ,test_hive_1713 string
        ,test_hive_1718 string
        ,test_hive_1717 string
        ,test_hive_1716 string
        ,test_hive_1719 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1722 purge;

create table if not exists test_hive_1722
(
max_partition bigint
);

drop view if exists test_hive_1725;

create view if not exists test_hive_1725
as
select
        cast(test_hive_1714 as int) as test_hive_1714
        ,cast(test_hive_1712 as int) as test_hive_1712
        ,cast(test_hive_1715 as int) as test_hive_1715
        ,cast(test_hive_434 as string) as test_hive_434
        ,cast(test_hive_1713 as string) as test_hive_1713
        ,cast(test_hive_1718 as string) as test_hive_1718
        ,cast(test_hive_1717 as string) as test_hive_1717
        ,cast(test_hive_1716 as string) as test_hive_1716
        ,cast(from_unixtime(unix_timestamp(test_hive_1719,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1719
       ,source_file_name
       ,cast(creation_date as timestamp) as creation_date
       ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1723
;

drop view if exists test_hive_1724;

create view test_hive_1724
as
select
        test_hive_1714 as test_hive_1714
        ,test_hive_1712 as test_hive_1712
        ,test_hive_1715 as test_hive_1715
        ,test_hive_434 as test_hive_434
        ,test_hive_1713 as test_hive_1713
        ,test_hive_1718 as test_hive_1718
        ,test_hive_1717 as test_hive_1717
        ,test_hive_1716 as test_hive_1716
        ,test_hive_1719 as test_hive_1719
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1725 t1
;

drop view if exists test_hive_1721;

create view test_hive_1721
as
select t1.*
from test_hive_1724 t1
inner join test_hive_1722 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1706 purge;

create table test_hive_1706
(
        test_hive_1700 string
        ,test_hive_1698 string
        ,test_hive_1701 string
        ,test_hive_433 string
        ,test_hive_1699 string
        ,test_hive_1704 string
        ,test_hive_1703 string
        ,test_hive_1702 string
        ,test_hive_1705 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1709
(
        test_hive_1700 string
        ,test_hive_1698 string
        ,test_hive_1701 string
        ,test_hive_433 string
        ,test_hive_1699 string
        ,test_hive_1704 string
        ,test_hive_1703 string
        ,test_hive_1702 string
        ,test_hive_1705 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1708 purge;

create table if not exists test_hive_1708
(
max_partition bigint
);

drop view if exists test_hive_1711;

create view if not exists test_hive_1711
as
select
        cast(test_hive_1700 as int) as test_hive_1700
        ,cast(test_hive_1698 as int) as test_hive_1698
        ,cast(test_hive_1701 as int) as test_hive_1701
        ,cast(test_hive_433 as string) as test_hive_433
        ,cast(test_hive_1699 as string) as test_hive_1699
        ,cast(test_hive_1704 as string) as test_hive_1704
        ,cast(test_hive_1703 as string) as test_hive_1703
        ,cast(test_hive_1702 as string) as test_hive_1702
        ,cast(from_unixtime(unix_timestamp(test_hive_1705,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1705
       ,source_file_name
       ,cast(creation_date as timestamp) as creation_date
       ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1709
;

drop view if exists test_hive_1710;

create view test_hive_1710
as
select
        test_hive_1700 as test_hive_1700
        ,test_hive_1698 as test_hive_1698
        ,test_hive_1701 as test_hive_1701
        ,test_hive_433 as test_hive_433
        ,test_hive_1699 as test_hive_1699
        ,test_hive_1704 as test_hive_1704
        ,test_hive_1703 as test_hive_1703
        ,test_hive_1702 as test_hive_1702
        ,test_hive_1705 as test_hive_1705
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1711 t1
;

drop view if exists test_hive_1707;

create view test_hive_1707
as
select t1.*
from test_hive_1710 t1
inner join test_hive_1708 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1692 purge;

create table test_hive_1692
(
        test_hive_1688 string
        ,test_hive_1686 string
        ,test_hive_1689 string
        ,test_hive_432 string
        ,test_hive_1687 string
        ,test_hive_1690 string
        ,test_hive_1691 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1695
(
        test_hive_1688 string
        ,test_hive_1686 string
        ,test_hive_1689 string
        ,test_hive_432 string
        ,test_hive_1687 string
        ,test_hive_1690 string
        ,test_hive_1691 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1694 purge;

create table if not exists test_hive_1694
(
max_partition bigint
);

drop view if exists test_hive_1697;

create view if not exists test_hive_1697
as
select
        cast(test_hive_1688 as int) as test_hive_1688
        ,cast(test_hive_1686 as int) as test_hive_1686
        ,cast(test_hive_1689 as int) as test_hive_1689
        ,cast(test_hive_432 as string) as test_hive_432
        ,cast(test_hive_1687 as string) as test_hive_1687
        ,cast(test_hive_1690 as string) as test_hive_1690
        ,cast(from_unixtime(unix_timestamp(test_hive_1691,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1691
       ,source_file_name
       ,cast(creation_date as timestamp) as creation_date
       ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1695
;

drop view if exists test_hive_1696;

create view test_hive_1696
as
select
        test_hive_1688 as test_hive_1688
        ,test_hive_1686 as test_hive_1686
        ,test_hive_1689 as test_hive_1689
        ,test_hive_432 as test_hive_432
        ,test_hive_1687 as test_hive_1687
        ,test_hive_1690 as test_hive_1690
        ,test_hive_1691 as test_hive_1691
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1697 t1
;

drop view if exists test_hive_1693;

create view test_hive_1693
as
select t1.*
from test_hive_1696 t1
inner join test_hive_1694 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1680 purge;

create table test_hive_1680
(
        test_hive_1676 string
        ,test_hive_1674 string
        ,test_hive_1677 string
        ,test_hive_431 string
        ,test_hive_1675 string
        ,test_hive_1678 string
        ,test_hive_1679 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1683
(
        test_hive_1676 string
        ,test_hive_1674 string
        ,test_hive_1677 string
        ,test_hive_431 string
        ,test_hive_1675 string
        ,test_hive_1678 string
        ,test_hive_1679 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1682 purge;

create table if not exists test_hive_1682
(
max_partition bigint
);

drop view if exists test_hive_1685;

create view if not exists test_hive_1685
as
select
        cast(test_hive_1676 as int) as test_hive_1676
        ,cast(test_hive_1674 as int) as test_hive_1674
        ,cast(test_hive_1677 as int) as test_hive_1677
        ,cast(test_hive_431 as string) as test_hive_431
        ,cast(test_hive_1675 as string) as test_hive_1675
        ,cast(test_hive_1678 as string) as test_hive_1678
        ,cast(from_unixtime(unix_timestamp(test_hive_1679,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1679
       ,source_file_name
       ,cast(creation_date as timestamp) as creation_date
       ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1683
;

drop view if exists test_hive_1684;

create view test_hive_1684
as
select
        test_hive_1676 as test_hive_1676
        ,test_hive_1674 as test_hive_1674
        ,test_hive_1677 as test_hive_1677
        ,test_hive_431 as test_hive_431
        ,test_hive_1675 as test_hive_1675
        ,test_hive_1678 as test_hive_1678
        ,test_hive_1679 as test_hive_1679
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1685 t1
;

drop view if exists test_hive_1681;

create view test_hive_1681
as
select t1.*
from test_hive_1684 t1
inner join test_hive_1682 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1668 purge;

create table test_hive_1668
(
        test_hive_1662 string
        ,test_hive_1660 string
        ,test_hive_1663 string
        ,test_hive_430 string
        ,test_hive_1661 string
        ,test_hive_1666 string
        ,test_hive_1665 string
        ,test_hive_1664 string
        ,test_hive_1667 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1671
(
        test_hive_1662 string
        ,test_hive_1660 string
        ,test_hive_1663 string
        ,test_hive_430 string
        ,test_hive_1661 string
        ,test_hive_1666 string
        ,test_hive_1665 string
        ,test_hive_1664 string
        ,test_hive_1667 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1670 purge;

create table if not exists test_hive_1670
(
max_partition bigint
);

drop view if exists test_hive_1673;

create view if not exists test_hive_1673
as
select
        cast(test_hive_1662 as int) as test_hive_1662
        ,cast(test_hive_1660 as int) as test_hive_1660
        ,cast(test_hive_1663 as int) as test_hive_1663
        ,cast(test_hive_430 as string) as test_hive_430
        ,cast(test_hive_1661 as string) as test_hive_1661
        ,cast(test_hive_1666 as string) as test_hive_1666
        ,cast(test_hive_1665 as string) as test_hive_1665
        ,cast(test_hive_1664 as string) as test_hive_1664
        ,cast(from_unixtime(unix_timestamp(test_hive_1667,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1667
       ,source_file_name
       ,cast(creation_date as timestamp) as creation_date
       ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1671
;

drop view if exists test_hive_1672;

create view test_hive_1672
as
select
        test_hive_1662 as test_hive_1662
        ,test_hive_1660 as test_hive_1660
        ,test_hive_1663 as test_hive_1663
        ,test_hive_430 as test_hive_430
        ,test_hive_1661 as test_hive_1661
        ,test_hive_1666 as test_hive_1666
        ,test_hive_1665 as test_hive_1665
        ,test_hive_1664 as test_hive_1664
        ,test_hive_1667 as test_hive_1667
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1673 t1
;

drop view if exists test_hive_1669;

create view test_hive_1669
as
select t1.*
from test_hive_1672 t1
inner join test_hive_1670 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1654 purge;

create table test_hive_1654
(
        test_hive_1650 string
        ,test_hive_1648 string
        ,test_hive_1651 string
        ,test_hive_429 string
        ,test_hive_1649 string
        ,test_hive_1652 string
        ,test_hive_1653 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1657
(
        test_hive_1650 string
        ,test_hive_1648 string
        ,test_hive_1651 string
        ,test_hive_429 string
        ,test_hive_1649 string
        ,test_hive_1652 string
        ,test_hive_1653 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1656 purge;

create table if not exists test_hive_1656
(
max_partition bigint
);

drop view if exists test_hive_1659;

create view if not exists test_hive_1659
as
select
        cast(test_hive_1650 as int) as test_hive_1650
        ,cast(test_hive_1648 as int) as test_hive_1648
        ,cast(test_hive_1651 as int) as test_hive_1651
        ,cast(test_hive_429 as string) as test_hive_429
        ,cast(test_hive_1649 as string) as test_hive_1649
        ,cast(test_hive_1652 as string) as test_hive_1652
        ,cast(from_unixtime(unix_timestamp(test_hive_1653,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1653
       ,source_file_name
       ,cast(creation_date as timestamp) as creation_date
       ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1657
;

drop view if exists test_hive_1658;

create view test_hive_1658
as
select
        test_hive_1650 as test_hive_1650
        ,test_hive_1648 as test_hive_1648
        ,test_hive_1651 as test_hive_1651
        ,test_hive_429 as test_hive_429
        ,test_hive_1649 as test_hive_1649
        ,test_hive_1652 as test_hive_1652
        ,test_hive_1653 as test_hive_1653
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1659 t1
;

drop view if exists test_hive_1655;

create view test_hive_1655
as
select t1.*
from test_hive_1658 t1
inner join test_hive_1656 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1642 purge;

create table test_hive_1642
(
        test_hive_1636 string
        ,test_hive_1634 string
        ,test_hive_1637 string
        ,test_hive_428 string
        ,test_hive_1635 string
        ,test_hive_1640 string
        ,test_hive_1639 string
        ,test_hive_1638 string
        ,test_hive_1641 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1645
(
        test_hive_1636 string
        ,test_hive_1634 string
        ,test_hive_1637 string
        ,test_hive_428 string
        ,test_hive_1635 string
        ,test_hive_1640 string
        ,test_hive_1639 string
        ,test_hive_1638 string
        ,test_hive_1641 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1644 purge;

create table if not exists test_hive_1644
(
max_partition bigint
);

drop view if exists test_hive_1647;

create view if not exists test_hive_1647
as
select
        cast(test_hive_1636 as int) as test_hive_1636
        ,cast(test_hive_1634 as int) as test_hive_1634
        ,cast(test_hive_1637 as int) as test_hive_1637
        ,cast(test_hive_428 as string) as test_hive_428
        ,cast(test_hive_1635 as string) as test_hive_1635
        ,cast(test_hive_1640 as string) as test_hive_1640
        ,cast(test_hive_1639 as string) as test_hive_1639
        ,cast(test_hive_1638 as string) as test_hive_1638
        ,cast(from_unixtime(unix_timestamp(test_hive_1641,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1641
       ,source_file_name
       ,cast(creation_date as timestamp) as creation_date
       ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1645
;

drop view if exists test_hive_1646;

create view test_hive_1646
as
select
        test_hive_1636 as test_hive_1636
        ,test_hive_1634 as test_hive_1634
        ,test_hive_1637 as test_hive_1637
        ,test_hive_428 as test_hive_428
        ,test_hive_1635 as test_hive_1635
        ,test_hive_1640 as test_hive_1640
        ,test_hive_1639 as test_hive_1639
        ,test_hive_1638 as test_hive_1638
        ,test_hive_1641 as test_hive_1641
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1647 t1
;

drop view if exists test_hive_1643;

create view test_hive_1643
as
select t1.*
from test_hive_1646 t1
inner join test_hive_1644 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1628 purge;

create table test_hive_1628
(
        test_hive_1622 string
        ,test_hive_1620 string
        ,test_hive_1623 string
        ,test_hive_427 string
        ,test_hive_1621 string
        ,test_hive_1626 string
        ,test_hive_1625 string
        ,test_hive_1624 string
        ,test_hive_1627 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1631
(
        test_hive_1622 string
        ,test_hive_1620 string
        ,test_hive_1623 string
        ,test_hive_427 string
        ,test_hive_1621 string
        ,test_hive_1626 string
        ,test_hive_1625 string
        ,test_hive_1624 string
        ,test_hive_1627 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1630 purge;

create table if not exists test_hive_1630
(
max_partition bigint
);

drop view if exists test_hive_1633;

create view if not exists test_hive_1633
as
select
        cast(test_hive_1622 as int) as test_hive_1622
        ,cast(test_hive_1620 as int) as test_hive_1620
        ,cast(test_hive_1623 as int) as test_hive_1623
        ,cast(test_hive_427 as string) as test_hive_427
        ,cast(test_hive_1621 as string) as test_hive_1621
        ,cast(test_hive_1626 as string) as test_hive_1626
        ,cast(test_hive_1625 as string) as test_hive_1625
        ,cast(test_hive_1624 as string) as test_hive_1624
        ,cast(from_unixtime(unix_timestamp(test_hive_1627,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1627
       ,source_file_name
       ,cast(creation_date as timestamp) as creation_date
       ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1631
;

drop view if exists test_hive_1632;

create view test_hive_1632
as
select
        test_hive_1622 as test_hive_1622
        ,test_hive_1620 as test_hive_1620
        ,test_hive_1623 as test_hive_1623
        ,test_hive_427 as test_hive_427
        ,test_hive_1621 as test_hive_1621
        ,test_hive_1626 as test_hive_1626
        ,test_hive_1625 as test_hive_1625
        ,test_hive_1624 as test_hive_1624
        ,test_hive_1627 as test_hive_1627
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1633 t1
;

drop view if exists test_hive_1629;

create view test_hive_1629
as
select t1.*
from test_hive_1632 t1
inner join test_hive_1630 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1614 purge;

create table test_hive_1614
(
        test_hive_1608 string
        ,test_hive_1606 string
        ,test_hive_1609 string
        ,test_hive_426 string
        ,test_hive_1607 string
        ,test_hive_1612 string
        ,test_hive_1611 string
        ,test_hive_1610 string
        ,test_hive_1613 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1617
(
        test_hive_1608 string
        ,test_hive_1606 string
        ,test_hive_1609 string
        ,test_hive_426 string
        ,test_hive_1607 string
        ,test_hive_1612 string
        ,test_hive_1611 string
        ,test_hive_1610 string
        ,test_hive_1613 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1616 purge;

create table if not exists test_hive_1616
(
max_partition bigint
);

drop view if exists test_hive_1619;

create view if not exists test_hive_1619
as
select
        cast(test_hive_1608 as int) as test_hive_1608
        ,cast(test_hive_1606 as int) as test_hive_1606
        ,cast(test_hive_1609 as int) as test_hive_1609
        ,cast(test_hive_426 as string) as test_hive_426
        ,cast(test_hive_1607 as string) as test_hive_1607
        ,cast(test_hive_1612 as string) as test_hive_1612
        ,cast(test_hive_1611 as string) as test_hive_1611
        ,cast(test_hive_1610 as string) as test_hive_1610
        ,cast(from_unixtime(unix_timestamp(test_hive_1613,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1613
       ,source_file_name
       ,cast(creation_date as timestamp) as creation_date
       ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1617
;

drop view if exists test_hive_1618;

create view test_hive_1618
as
select
        test_hive_1608 as test_hive_1608
        ,test_hive_1606 as test_hive_1606
        ,test_hive_1609 as test_hive_1609
        ,test_hive_426 as test_hive_426
        ,test_hive_1607 as test_hive_1607
        ,test_hive_1612 as test_hive_1612
        ,test_hive_1611 as test_hive_1611
        ,test_hive_1610 as test_hive_1610
        ,test_hive_1613 as test_hive_1613
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1619 t1
;

drop view if exists test_hive_1615;

create view test_hive_1615
as
select t1.*
from test_hive_1618 t1
inner join test_hive_1616 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1600 purge;

create table test_hive_1600
(
        test_hive_1596 string
        ,test_hive_1594 string
        ,test_hive_1597 string
        ,test_hive_425 string
        ,test_hive_1595 string
        ,test_hive_1598 string
        ,test_hive_1599 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1603
(
        test_hive_1596 string
        ,test_hive_1594 string
        ,test_hive_1597 string
        ,test_hive_425 string
        ,test_hive_1595 string
        ,test_hive_1598 string
        ,test_hive_1599 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1602 purge;

create table if not exists test_hive_1602
(
max_partition bigint
);

drop view if exists test_hive_1605;

create view if not exists test_hive_1605
as
select
        cast(test_hive_1596 as int) as test_hive_1596
        ,cast(test_hive_1594 as int) as test_hive_1594
        ,cast(test_hive_1597 as int) as test_hive_1597
        ,cast(test_hive_425 as string) as test_hive_425
        ,cast(test_hive_1595 as string) as test_hive_1595
        ,cast(test_hive_1598 as string) as test_hive_1598
        ,cast(from_unixtime(unix_timestamp(test_hive_1599,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1599
       ,source_file_name
       ,cast(creation_date as timestamp) as creation_date
       ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1603
;

drop view if exists test_hive_1604;

create view test_hive_1604
as
select
        test_hive_1596 as test_hive_1596
        ,test_hive_1594 as test_hive_1594
        ,test_hive_1597 as test_hive_1597
        ,test_hive_425 as test_hive_425
        ,test_hive_1595 as test_hive_1595
        ,test_hive_1598 as test_hive_1598
        ,test_hive_1599 as test_hive_1599
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1605 t1
;

drop view if exists test_hive_1601;

create view test_hive_1601
as
select t1.*
from test_hive_1604 t1
inner join test_hive_1602 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1588 purge;

create table test_hive_1588
(
        test_hive_1584 string
        ,test_hive_1582 string
        ,test_hive_1585 string
        ,test_hive_424 string
        ,test_hive_1583 string
        ,test_hive_1586 string
        ,test_hive_1587 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1591
(
        test_hive_1584 string
        ,test_hive_1582 string
        ,test_hive_1585 string
        ,test_hive_424 string
        ,test_hive_1583 string
        ,test_hive_1586 string
        ,test_hive_1587 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1590 purge;

create table if not exists test_hive_1590
(
max_partition bigint
);

drop view if exists test_hive_1593;

create view if not exists test_hive_1593
as
select
        cast(test_hive_1584 as int) as test_hive_1584
        ,cast(test_hive_1582 as int) as test_hive_1582
        ,cast(test_hive_1585 as int) as test_hive_1585
        ,cast(test_hive_424 as string) as test_hive_424
        ,cast(test_hive_1583 as string) as test_hive_1583
        ,cast(test_hive_1586 as string) as test_hive_1586
        ,cast(from_unixtime(unix_timestamp(test_hive_1587,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1587
       ,source_file_name
       ,cast(creation_date as timestamp) as creation_date
       ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1591
;

drop view if exists test_hive_1592;

create view test_hive_1592
as
select
        test_hive_1584 as test_hive_1584
        ,test_hive_1582 as test_hive_1582
        ,test_hive_1585 as test_hive_1585
        ,test_hive_424 as test_hive_424
        ,test_hive_1583 as test_hive_1583
        ,test_hive_1586 as test_hive_1586
        ,test_hive_1587 as test_hive_1587
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1593 t1
;

drop view if exists test_hive_1589;

create view test_hive_1589
as
select t1.*
from test_hive_1592 t1
inner join test_hive_1590 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1576 purge;

create table test_hive_1576
(
        test_hive_1570 string
        ,test_hive_1568 string
        ,test_hive_1571 string
        ,test_hive_423 string
        ,test_hive_1569 string
        ,test_hive_1574 string
        ,test_hive_1573 string
        ,test_hive_1572 string
        ,test_hive_1575 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1579
(
        test_hive_1570 string
        ,test_hive_1568 string
        ,test_hive_1571 string
        ,test_hive_423 string
        ,test_hive_1569 string
        ,test_hive_1574 string
        ,test_hive_1573 string
        ,test_hive_1572 string
        ,test_hive_1575 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1578 purge;

create table if not exists test_hive_1578
(
max_partition bigint
);

drop view if exists test_hive_1581;

create view if not exists test_hive_1581
as
select
        cast(test_hive_1570 as int) as test_hive_1570
        ,cast(test_hive_1568 as int) as test_hive_1568
        ,cast(test_hive_1571 as int) as test_hive_1571
        ,cast(test_hive_423 as string) as test_hive_423
        ,cast(test_hive_1569 as string) as test_hive_1569
        ,cast(test_hive_1574 as string) as test_hive_1574
        ,cast(test_hive_1573 as string) as test_hive_1573
        ,cast(test_hive_1572 as string) as test_hive_1572
        ,cast(from_unixtime(unix_timestamp(test_hive_1575,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1575
       ,source_file_name
       ,cast(creation_date as timestamp) as creation_date
       ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1579
;

drop view if exists test_hive_1580;

create view test_hive_1580
as
select
        test_hive_1570 as test_hive_1570
        ,test_hive_1568 as test_hive_1568
        ,test_hive_1571 as test_hive_1571
        ,test_hive_423 as test_hive_423
        ,test_hive_1569 as test_hive_1569
        ,test_hive_1574 as test_hive_1574
        ,test_hive_1573 as test_hive_1573
        ,test_hive_1572 as test_hive_1572
        ,test_hive_1575 as test_hive_1575
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1581 t1
;

drop view if exists test_hive_1577;

create view test_hive_1577
as
select t1.*
from test_hive_1580 t1
inner join test_hive_1578 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1562 purge;

create table test_hive_1562
(
        test_hive_1558 string
        ,test_hive_1556 string
        ,test_hive_1559 string
        ,test_hive_422 string
        ,test_hive_1557 string
        ,test_hive_1560 string
        ,test_hive_1561 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1565
(
        test_hive_1558 string
        ,test_hive_1556 string
        ,test_hive_1559 string
        ,test_hive_422 string
        ,test_hive_1557 string
        ,test_hive_1560 string
        ,test_hive_1561 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1564 purge;

create table if not exists test_hive_1564
(
max_partition bigint
);

drop view if exists test_hive_1567;

create view if not exists test_hive_1567
as
select
        cast(test_hive_1558 as int) as test_hive_1558
        ,cast(test_hive_1556 as int) as test_hive_1556
        ,cast(test_hive_1559 as int) as test_hive_1559
        ,cast(test_hive_422 as string) as test_hive_422
        ,cast(test_hive_1557 as string) as test_hive_1557
        ,cast(test_hive_1560 as string) as test_hive_1560
        ,cast(from_unixtime(unix_timestamp(test_hive_1561,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1561
       ,source_file_name
       ,cast(creation_date as timestamp) as creation_date
       ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1565
;

drop view if exists test_hive_1566;

create view test_hive_1566
as
select
        test_hive_1558 as test_hive_1558
        ,test_hive_1556 as test_hive_1556
        ,test_hive_1559 as test_hive_1559
        ,test_hive_422 as test_hive_422
        ,test_hive_1557 as test_hive_1557
        ,test_hive_1560 as test_hive_1560
        ,test_hive_1561 as test_hive_1561
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1567 t1
;

drop view if exists test_hive_1563;

create view test_hive_1563
as
select t1.*
from test_hive_1566 t1
inner join test_hive_1564 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1550 purge;

create table test_hive_1550
(
        test_hive_1545 string
        ,test_hive_1543 string
        ,test_hive_1546 string
        ,test_hive_421 string
        ,test_hive_1544 string
        ,test_hive_1548 string
        ,test_hive_1547 string
        ,test_hive_1549 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1553
(
        test_hive_1545 string
        ,test_hive_1543 string
        ,test_hive_1546 string
        ,test_hive_421 string
        ,test_hive_1544 string
        ,test_hive_1548 string
        ,test_hive_1547 string
        ,test_hive_1549 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1552 purge;

create table if not exists test_hive_1552
(
max_partition bigint
);

drop view if exists test_hive_1555;

create view if not exists test_hive_1555
as
select
        cast(test_hive_1545 as int) as test_hive_1545
        ,cast(test_hive_1543 as int) as test_hive_1543
        ,cast(test_hive_1546 as int) as test_hive_1546
        ,cast(test_hive_421 as string) as test_hive_421
        ,cast(test_hive_1544 as string) as test_hive_1544
        ,cast(test_hive_1548 as string) as test_hive_1548
        ,cast(test_hive_1547 as string) as test_hive_1547
        ,cast(from_unixtime(unix_timestamp(test_hive_1549,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1549
       ,source_file_name
       ,cast(creation_date as timestamp) as creation_date
       ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1553
;

drop view if exists test_hive_1554;

create view test_hive_1554
as
select
        test_hive_1545 as test_hive_1545
        ,test_hive_1543 as test_hive_1543
        ,test_hive_1546 as test_hive_1546
        ,test_hive_421 as test_hive_421
        ,test_hive_1544 as test_hive_1544
        ,test_hive_1548 as test_hive_1548
        ,test_hive_1547 as test_hive_1547
        ,test_hive_1549 as test_hive_1549
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1555 t1
;

drop view if exists test_hive_1551;

create view test_hive_1551
as
select t1.*
from test_hive_1554 t1
inner join test_hive_1552 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1328 purge;

create table test_hive_1328
(
        test_hive_1322 string
        ,test_hive_1318 string
        ,test_hive_1323 string
        ,test_hive_335 string
        ,test_hive_1321 string
        ,test_hive_1320 string
        ,test_hive_1319 string
        ,test_hive_1326 string
        ,test_hive_1325 string
        ,test_hive_1324 string
        ,test_hive_1327 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1331
(
        test_hive_1322 string
        ,test_hive_1318 string
        ,test_hive_1323 string
        ,test_hive_335 string
        ,test_hive_1321 string
        ,test_hive_1320 string
        ,test_hive_1319 string
        ,test_hive_1326 string
        ,test_hive_1325 string
        ,test_hive_1324 string
        ,test_hive_1327 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1330 purge;

create table if not exists test_hive_1330
(
max_partition bigint
);

drop view if exists test_hive_1333;

create view if not exists test_hive_1333
as
select
        cast(test_hive_1322 as int) as test_hive_1322
        ,cast(test_hive_1318 as int) as test_hive_1318
        ,cast(test_hive_1323 as int) as test_hive_1323
        ,cast(test_hive_335 as string) as test_hive_335
        ,cast(test_hive_1321 as string) as test_hive_1321
        ,cast(from_unixtime(unix_timestamp(test_hive_1320 ,'yyyymmdd'), 'yyyy-mm-dd') as timestamp) as test_hive_1320
        ,cast(from_unixtime(unix_timestamp(test_hive_1319 ,'yyyymmdd'), 'yyyy-mm-dd') as timestamp) as test_hive_1319
        ,cast(test_hive_1326 as string) as test_hive_1326
        ,cast(test_hive_1325 as string) as test_hive_1325
        ,cast(test_hive_1324 as string) as test_hive_1324
        ,cast(from_unixtime(unix_timestamp(test_hive_1327,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1327
       ,source_file_name
       ,cast(creation_date as timestamp) as creation_date
       ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1331
;

drop view if exists test_hive_1332;

create view test_hive_1332
as
select
        test_hive_1322 as test_hive_1322
        ,test_hive_1318 as test_hive_1318
        ,test_hive_1323 as test_hive_1323
        ,test_hive_335 as test_hive_335
        ,test_hive_1321 as test_hive_1321
        ,test_hive_1320 as test_hive_1320
        ,test_hive_1319 as test_hive_1319
        ,test_hive_1326 as test_hive_1326
        ,test_hive_1325 as test_hive_1325
        ,test_hive_1324 as test_hive_1324
        ,test_hive_1327 as test_hive_1327
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1333 t1
;

drop view if exists test_hive_1329;

create view test_hive_1329
as
select t1.*
from test_hive_1332 t1
inner join test_hive_1330 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1276 purge;

create table test_hive_1276
(
        test_hive_1272 string
        ,test_hive_1270 string
        ,test_hive_1273 string
        ,test_hive_308 string
        ,test_hive_1271 string
        ,test_hive_1274 string
        ,test_hive_1275 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1279
(
        test_hive_1272 string
        ,test_hive_1270 string
        ,test_hive_1273 string
        ,test_hive_308 string
        ,test_hive_1271 string
        ,test_hive_1274 string
        ,test_hive_1275 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1278 purge;

create table if not exists test_hive_1278
(
max_partition bigint
);

drop view if exists test_hive_1281;

create view if not exists test_hive_1281
as
select
        cast(test_hive_1272 as int) as test_hive_1272
        ,cast(test_hive_1270 as int) as test_hive_1270
        ,cast(test_hive_1273 as int) as test_hive_1273
        ,cast(test_hive_308 as string) as test_hive_308
        ,cast(test_hive_1271 as string) as test_hive_1271
        ,cast(test_hive_1274 as string) as test_hive_1274
        ,cast(from_unixtime(unix_timestamp(test_hive_1275,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1275
       ,source_file_name
       ,cast(creation_date as timestamp) as creation_date
       ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1279
;

drop view if exists test_hive_1280;

create view test_hive_1280
as
select
        test_hive_1272 as test_hive_1272
        ,test_hive_1270 as test_hive_1270
        ,test_hive_1273 as test_hive_1273
        ,test_hive_308 as test_hive_308
        ,test_hive_1271 as test_hive_1271
        ,test_hive_1274 as test_hive_1274
        ,test_hive_1275 as test_hive_1275
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1281 t1
;

drop view if exists test_hive_1277;

create view test_hive_1277
as
select t1.*
from test_hive_1280 t1
inner join test_hive_1278 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1264 purge;

create table test_hive_1264
(
        test_hive_1258 string
        ,test_hive_1256 string
        ,test_hive_1259 string
        ,test_hive_307 string
        ,test_hive_306 string
        ,test_hive_1257 string
        ,test_hive_1262 string
        ,test_hive_1261 string
        ,test_hive_1260 string
        ,test_hive_1263 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1267
(
        test_hive_1258 string
        ,test_hive_1256 string
        ,test_hive_1259 string
        ,test_hive_307 string
        ,test_hive_306 string
        ,test_hive_1257 string
        ,test_hive_1262 string
        ,test_hive_1261 string
        ,test_hive_1260 string
        ,test_hive_1263 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1266 purge;

create table if not exists test_hive_1266
(
max_partition bigint
);

drop view if exists test_hive_1269;

create view if not exists test_hive_1269
as
select
        cast(test_hive_1258 as int) as test_hive_1258
        ,cast(test_hive_1256 as int) as test_hive_1256
        ,cast(test_hive_1259 as int) as test_hive_1259
        ,cast(test_hive_307 as string) as test_hive_307
        ,cast(test_hive_306 as string) as test_hive_306
        ,cast(test_hive_1257 as string) as test_hive_1257
        ,cast(test_hive_1262 as string) as test_hive_1262
        ,cast(test_hive_1261 as string) as test_hive_1261
        ,cast(test_hive_1260 as string) as test_hive_1260
        ,cast(from_unixtime(unix_timestamp(test_hive_1263,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1263
       ,source_file_name
       ,cast(creation_date as timestamp) as creation_date
       ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1267
;

drop view if exists test_hive_1268;

create view test_hive_1268
as
select
        test_hive_1258 as test_hive_1258
        ,test_hive_1256 as test_hive_1256
        ,test_hive_1259 as test_hive_1259
        ,test_hive_307 as test_hive_307
        ,test_hive_306 as test_hive_306
        ,test_hive_1257 as test_hive_1257
        ,test_hive_1262 as test_hive_1262
        ,test_hive_1261 as test_hive_1261
        ,test_hive_1260 as test_hive_1260
        ,test_hive_1263 as test_hive_1263
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1269 t1
;

drop view if exists test_hive_1265;

create view test_hive_1265
as
select t1.*
from test_hive_1268 t1
inner join test_hive_1266 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1226 purge;

create table test_hive_1226
(
        test_hive_1222 string
        ,test_hive_1220 string
        ,test_hive_1223 string
        ,test_hive_280 string
        ,test_hive_1221 string
        ,test_hive_1224 string
        ,test_hive_1225 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1229
(
        test_hive_1222 string
        ,test_hive_1220 string
        ,test_hive_1223 string
        ,test_hive_280 string
        ,test_hive_1221 string
        ,test_hive_1224 string
        ,test_hive_1225 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1228 purge;

create table if not exists test_hive_1228
(
max_partition bigint
);

drop view if exists test_hive_1231;

create view if not exists test_hive_1231
as
select
        cast(test_hive_1222 as int) as test_hive_1222
        ,cast(test_hive_1220 as int) as test_hive_1220
        ,cast(test_hive_1223 as int) as test_hive_1223
        ,cast(test_hive_280 as string) as test_hive_280
        ,cast(test_hive_1221 as string) as test_hive_1221
        ,cast(test_hive_1224 as string) as test_hive_1224
        ,cast(from_unixtime(unix_timestamp(test_hive_1225,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1225
       ,source_file_name
       ,cast(creation_date as timestamp) as creation_date
       ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1229
;

drop view if exists test_hive_1230;

create view test_hive_1230
as
select
        test_hive_1222 as test_hive_1222
        ,test_hive_1220 as test_hive_1220
        ,test_hive_1223 as test_hive_1223
        ,test_hive_280 as test_hive_280
        ,test_hive_1221 as test_hive_1221
        ,test_hive_1224 as test_hive_1224
        ,test_hive_1225 as test_hive_1225
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1231 t1
;

drop view if exists test_hive_1227;

create view test_hive_1227
as
select t1.*
from test_hive_1230 t1
inner join test_hive_1228 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_1214 purge;

create table test_hive_1214
(
        test_hive_1210 string
        ,test_hive_1208 string
        ,test_hive_1211 string
        ,test_hive_279 string
        ,test_hive_1209 string
        ,test_hive_1212 string
        ,test_hive_1213 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_1217
(
        test_hive_1210 string
        ,test_hive_1208 string
        ,test_hive_1211 string
        ,test_hive_279 string
        ,test_hive_1209 string
        ,test_hive_1212 string
        ,test_hive_1213 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_1216 purge;

create table if not exists test_hive_1216
(
max_partition bigint
);

drop view if exists test_hive_1219;

create view if not exists test_hive_1219
as
select
        cast(test_hive_1210 as int) as test_hive_1210
        ,cast(test_hive_1208 as int) as test_hive_1208
        ,cast(test_hive_1211 as int) as test_hive_1211
        ,cast(test_hive_279 as string) as test_hive_279
        ,cast(test_hive_1209 as string) as test_hive_1209
        ,cast(test_hive_1212 as string) as test_hive_1212
        ,cast(from_unixtime(unix_timestamp(test_hive_1213,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_1213
       ,source_file_name
       ,cast(creation_date as timestamp) as creation_date
       ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_1217
;

drop view if exists test_hive_1218;

create view test_hive_1218
as
select
        test_hive_1210 as test_hive_1210
        ,test_hive_1208 as test_hive_1208
        ,test_hive_1211 as test_hive_1211
        ,test_hive_279 as test_hive_279
        ,test_hive_1209 as test_hive_1209
        ,test_hive_1212 as test_hive_1212
        ,test_hive_1213 as test_hive_1213
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_1219 t1
;

drop view if exists test_hive_1215;

create view test_hive_1215
as
select t1.*
from test_hive_1218 t1
inner join test_hive_1216 t2 on
t1.ds_ts =  t2.max_partition;
drop table if exists test_hive_2046 purge;

create table test_hive_2046
(
        test_hive_2043 string
        ,test_hive_2034 string
        ,test_hive_2044 string
        ,test_hive_2033 string
        ,test_hive_459 string
        ,test_hive_460 string
        ,test_hive_461 string
        ,test_hive_462 string
        ,test_hive_463 string
        ,test_hive_464 string
        ,test_hive_465 string
        ,test_hive_2035 string
        ,test_hive_2036 string
        ,test_hive_2037 string
        ,test_hive_2038 string
        ,test_hive_2039 string
        ,test_hive_2040 string
        ,test_hive_2041 string
        ,test_hive_2042 string
        ,test_hive_467 string
        ,test_hive_468 string
        ,test_hive_469 string
        ,test_hive_466 string
        ,test_hive_2045 string
)
partitioned by (ds int, ts int)
row format delimited fields terminated by '31'
tblproperties('serialization.null.format' = '');


create table if not exists test_hive_2049
(
        test_hive_2043 string
        ,test_hive_2034 string
        ,test_hive_2044 string
        ,test_hive_2033 string
        ,test_hive_459 string
        ,test_hive_460 string
        ,test_hive_461 string
        ,test_hive_462 string
        ,test_hive_463 string
        ,test_hive_464 string
        ,test_hive_465 string
        ,test_hive_2035 string
        ,test_hive_2036 string
        ,test_hive_2037 string
        ,test_hive_2038 string
        ,test_hive_2039 string
        ,test_hive_2040 string
        ,test_hive_2041 string
        ,test_hive_2042 string
        ,test_hive_467 string
        ,test_hive_468 string
        ,test_hive_469 string
        ,test_hive_466 string
        ,test_hive_2045 string
        ,source_file_name string
        ,creation_date string
        ,ds_ts bigint
        ,ts int
)
partitioned by (ds int)
stored as parquet;

drop table if exists test_hive_2048 purge;

create table if not exists test_hive_2048
(
max_partition bigint
);

drop view if exists test_hive_2051;

create view if not exists test_hive_2051
as
select
        cast(test_hive_2043 as int) as test_hive_2043
        ,cast(test_hive_2034 as int) as test_hive_2034
        ,cast(test_hive_2044 as int) as test_hive_2044
        ,cast(test_hive_2033 as string) as test_hive_2033
        ,cast(test_hive_459 as string) as test_hive_459
        ,cast(test_hive_460 as string) as test_hive_460
        ,cast(test_hive_461 as string) as test_hive_461
        ,cast(test_hive_462 as string) as test_hive_462
        ,cast(test_hive_463 as string) as test_hive_463
        ,cast(test_hive_464 as string) as test_hive_464
        ,cast(test_hive_465 as string) as test_hive_465
        ,cast(test_hive_2035 as int) as test_hive_2035
        ,cast(test_hive_2036 as int) as test_hive_2036
        ,cast(test_hive_2037 as int) as test_hive_2037
        ,cast(test_hive_2038 as int) as test_hive_2038
        ,cast(test_hive_2039 as int) as test_hive_2039
        ,cast(test_hive_2040 as int) as test_hive_2040
        ,cast(test_hive_2041 as int) as test_hive_2041
        ,cast(test_hive_2042 as int) as test_hive_2042
        ,cast(test_hive_467 as string) as test_hive_467
        ,cast(test_hive_468 as string) as test_hive_468
        ,cast(test_hive_469 as string) as test_hive_469
        ,cast(test_hive_466 as string) as test_hive_466
        ,cast(from_unixtime(unix_timestamp(test_hive_2045,'yyyymmddhhmmss'), 'yyyy-mm-dd hh:mm:ss') as timestamp) as test_hive_2045
       ,source_file_name
       ,cast(creation_date as timestamp) as creation_date
       ,ds_ts
        ,cast(ds as bigint) as ds
        ,cast(ts as bigint) as ts
from test_hive_2049
;

drop view if exists test_hive_2050;

create view test_hive_2050
as
select
        test_hive_2043 as test_hive_2043
        ,test_hive_2034 as test_hive_2034
        ,test_hive_2044 as test_hive_2044
        ,test_hive_2033 as test_hive_2033
        ,test_hive_459 as test_hive_459
        ,test_hive_460 as test_hive_460
        ,test_hive_461 as test_hive_461
        ,test_hive_462 as test_hive_462
        ,test_hive_463 as test_hive_463
        ,test_hive_464 as test_hive_464
        ,test_hive_465 as test_hive_465
        ,test_hive_2035 as test_hive_2035
        ,test_hive_2036 as test_hive_2036
        ,test_hive_2037 as test_hive_2037
        ,test_hive_2038 as test_hive_2038
        ,test_hive_2039 as test_hive_2039
        ,test_hive_2040 as test_hive_2040
        ,test_hive_2041 as test_hive_2041
        ,test_hive_2042 as test_hive_2042
        ,test_hive_467 as test_hive_467
        ,test_hive_468 as test_hive_468
        ,test_hive_469 as test_hive_469
        ,test_hive_466 as test_hive_466
        ,test_hive_2045 as test_hive_2045
        ,source_file_name
        ,creation_date
        ,ds_ts
        ,ts
            ,ds
from test_hive_2051 t1
;

drop view if exists test_hive_2047;

create view test_hive_2047
as
select t1.*
from test_hive_2050 t1
inner join test_hive_2048 t2 on
t1.ds_ts =  t2.max_partition;

set hive.stageid.rearrange=execution;
set hive.auto.convert.join=true;
set hive.cbo.enable=false;

explain select
t1.test_hive_1018,
t1.test_hive_1004,
t1.test_hive_1025,
t2.test_hive_1560,
t4.test_hive_1274,
t1.test_hive_29,
t7.test_hive_1948,
t1.test_hive_97,
t32.test_hive_1610,
t1.test_hive_98,
t34.test_hive_1972,
t35.test_hive_1792,
t41.test_hive_1224,
t43.test_hive_1895,
t44.test_hive_1907,
t45.test_hive_1935,
t46.test_hive_2010,
t47.test_hive_2023,
t1.test_hive_78,
t15.test_hive_1260,
t1.test_hive_79,
t1.test_hive_24,
t3.test_hive_1716,
t42.test_hive_1224,
t14.test_hive_1198,
t23.test_hive_1459,
t28.test_hive_1533,
t26.test_hive_1503,
t11.test_hive_1154,
t21.test_hive_1429,
t17.test_hive_1340,
t18.test_hive_1356,
t38.test_hive_1847,
t39.test_hive_1859,
t40.test_hive_1871,
t12.test_hive_1168,
t22.test_hive_1443,
t13.test_hive_1182,
t25.test_hive_1487,
t24.test_hive_1473,
t27.test_hive_1517,
t8.test_hive_1110,
t9.test_hive_1124,
t10.test_hive_1138,
t16.test_hive_1309,
t36.test_hive_1806,
t1.test_hive_104,
t1.test_hive_1002,
t1.test_hive_1003,
t1.test_hive_25,
t5.test_hive_1960,
t29.test_hive_1547,
t30.test_hive_1224,
t31.test_hive_1224,
t33.test_hive_1778,
t37.test_hive_1834,
t19.test_hive_1972,
t20.test_hive_1972,
t1.test_hive_100,
t1.test_hive_1023,
t1.test_hive_1024,
t1.test_hive_1010,
t1.test_hive_1010_a_d,
t1.test_hive_1010_a_g,
t1.test_hive_1026,
t1.test_hive_1000,
t1.test_hive_1001,
t1.test_hive_1030,
t1.test_hive_1030_1,
t1.test_hive_1030_2,
t1.test_hive_1030_3,
t1.test_hive_1021,
t1.test_hive_1020,
t1.test_hive_1022,
t1.test_hive_1019,
t1.test_hive_1027,
t1.test_hive_1028,
t1.test_hive_1029,
t1.test_hive_1005,
t1.test_hive_1005_a_d,
t1.test_hive_1005_psr,
t1.test_hive_1005_psr_a_d,
t1.test_hive_1005_psr_e,
t1.test_hive_1013,
t1.test_hive_1013_a_d,
t1.test_hive_1013_psr,
t1.test_hive_1013_psr_a_d,
t1.test_hive_1013_psr_e,
t1.test_hive_1034
from test_hive_1036 t1
join test_hive_1563 t2 on t1.test_hive_23 = t2.test_hive_422
join test_hive_1721 t3 on t1.test_hive_26 = t3.test_hive_434
join test_hive_1277 t4 on t1.test_hive_27 = t4.test_hive_308
join test_hive_1963 t5 on t1.test_hive_28 = t5.test_hive_453
join test_hive_1951 t7 on t1.test_hive_30 = t7.test_hive_452
join test_hive_1115 t8 on t1.test_hive_71 = t8.test_hive_272
join test_hive_1129 t9 on t1.test_hive_72 = t9.test_hive_273
join test_hive_1143 t10 on t1.test_hive_73 = t10.test_hive_274
join test_hive_1159 t11 on t1.test_hive_74 = t11.test_hive_275
join test_hive_1173 t12 on t1.test_hive_75 = t12.test_hive_276
join test_hive_1187 t13 on t1.test_hive_76 = t13.test_hive_277
join test_hive_1203 t14 on t1.test_hive_77 = t14.test_hive_278
join test_hive_1265 t15 on t1.test_hive_78 = t15.test_hive_306
join test_hive_1313 t16 on t1.test_hive_80 = t16.test_hive_334
join test_hive_1345 t17 on t1.test_hive_81 = t17.test_hive_336
join test_hive_1361 t18 on t1.test_hive_82 = t18.test_hive_337
join test_hive_1977 t19 on t1.test_hive_83 = t19.test_hive_454
join test_hive_1977 t20 on t1.test_hive_84 = t20.test_hive_454
join test_hive_1434 t21 on t1.test_hive_85 = t21.test_hive_413
join test_hive_1448 t22 on t1.test_hive_86 = t22.test_hive_414
join test_hive_1464 t23 on t1.test_hive_87 = t23.test_hive_415
join test_hive_1478 t24 on t1.test_hive_88 = t24.test_hive_416
join test_hive_1492 t25 on t1.test_hive_89 = t25.test_hive_417
join test_hive_1508 t26 on t1.test_hive_90 = t26.test_hive_418
join test_hive_1522 t27 on t1.test_hive_91 = t27.test_hive_419
join test_hive_1538 t28 on t1.test_hive_92 = t28.test_hive_420
join test_hive_1551 t29 on t1.test_hive_93 = t29.test_hive_421
join test_hive_1227 t30 on t1.test_hive_94 = t30.test_hive_280
join test_hive_1227 t31 on t1.test_hive_95 = t31.test_hive_280
join test_hive_1615 t32 on t1.test_hive_96 = t32.test_hive_426
join test_hive_1783 t33 on t1.test_hive_99 = t33.test_hive_440
join test_hive_1977 t34 on t1.test_hive_101 = t34.test_hive_454
join test_hive_1797 t35 on t1.test_hive_102 = t35.test_hive_441
join test_hive_1811 t36 on t1.test_hive_103 = t36.test_hive_442
join test_hive_1838 t37 on t1.test_hive_105 = t37.test_hive_444
join test_hive_1850 t38 on t1.test_hive_106 = t38.test_hive_445
join test_hive_1862 t39 on t1.test_hive_107 = t39.test_hive_446
join test_hive_1874 t40 on t1.test_hive_108 = t40.test_hive_447
join test_hive_1227 t41 on t1.test_hive_109 = t41.test_hive_280
join test_hive_1227 t42 on t1.test_hive_110 = t42.test_hive_280
join test_hive_1898 t43 on t1.test_hive_111 = t43.test_hive_449
join test_hive_1911 t44 on t1.test_hive_112 = t44.test_hive_450
join test_hive_1939 t45 on t1.test_hive_113 = t45.test_hive_451
join test_hive_2014 t46 on t1.test_hive_114 = t46.test_hive_457
join test_hive_2028 t47 on t1.test_hive_115 = t47.test_hive_458
;
