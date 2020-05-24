create database cpn;
create database cpn_view;

drop table if exists cpn.mytable_1000_n;
CREATE external TABLE IF NOT EXISTS cpn.mytable_1000_n (
col0 decimal(18,0),
col1 decimal(18,0),
col2 char(1),
col4 decimal(18,0),
col5 char(1),
col6 char(1),
col7 decimal(18,0),
col8 decimal(18,5),
col9 decimal(28,0),
col10 int,
col13 decimal(18,0),
col15 string,
col16 timestamp,
col17 string,
col18 string,
col19 char(1),
col21 char(10),
col20 char(1),
col22 char(3),
col23 timestamp,
col11 char(3),
col24 decimal(18,9),
col25 decimal(18,5),
col26 char(1),
col27 decimal(18,5),
col28 char(5),
col75 string,
col29 char(1),
col30 decimal(18,5),
col31 decimal(18,5),
col72 int,
col32 decimal(18,0),
col33 char(1),
col34 char(5),
col35 string,
col36 string,
col37 decimal(18,0),
col38 tinyint,
col73 string,
col39 char(1),
parent_col9 decimal(28,0),
col40 tinyint,
col41 tinyint,
col42 decimal(18,0),
col89 string,
col90 char(1),
col91 decimal(18,0),
col83 string,
col81 decimal(18,0),
col92 decimal(18,0),
col93 decimal(18,0),
col94 string,
col80 int,
col95 decimal(18,5),
col96 string,
col97 char(1),
col78 decimal(18,0),
col98 tinyint,
col87 string,
col99 decimal(18,5),
col100 string,
col101 char(1),
col14 decimal(18,0),
col102 tinyint,
col85 string,
col103 decimal(18,5),
col104 string,
col105 char(1),
col67 decimal(18,0),
col79 tinyint,
col106 string,
col107 timestamp,
col43 char(10),
col82 decimal(18,0),
col84 decimal(18,0),
col108 decimal(18,0),
col109 decimal(18,0),
col110 date,
col111 date,
col86 int,
col88 int,
col112 tinyint,
col113 tinyint,
col44 string,
col45 string,
col46 char(1),
col47 string,
col48 decimal(18,0),
col49 decimal(18,0),
col50 tinyint,
col51 string,
col12 char(3),
col52 decimal(18,9),
col53 decimal(18,5),
col54 decimal(18,5),
col55 char(1),
col56 tinyint,
col57 string,
col58 int,
col59 char(5),
col60 decimal(18,0),
col76 decimal(28,0),
col61 string,
col62 string,
col74 decimal(18,5),
col65 int,
col63 smallint,
col64 char(1),
col66 string,
col68 int,
col69 string,
col114 date,
col115 decimal(18,0),
col70 string,
col71 string,
col77 int,
col116 timestamp,
col117 timestamp)
partitioned by (col3 date)
stored as orc;

EXPLAIN CBO
CREATE OR REPLACE VIEW cpn_view.myview_1000_n AS
SELECT
col0
,col1
,col2
,col3
,col4
,col5
,col6
,col7
,COALESCE(col8,0.0) AS col8
,COALESCE(col9,0) as col9
,col10
,CASE WHEN COALESCE(col11,'$')='X' THEN 'Y' ELSE col11 END as Alt_col11
,CASE WHEN COALESCE(col12,'$')='X' THEN 'Y' ELSE col12 END as Alt_col12
,col13
,(Case WHEN trim(col5) IN ('A','B','C') THEN COALESCE(col14,-1) ELSE COALESCE(col14,1) END) AS col14
,col15
,col16
,col17
,col18
,col19
,col20
,col21
,col22
,col23
,COALESCE(trim(col11),'Z') AS col11
,col24
,col25
,col26
,col27
,col28
,col29
,col30
,col31
,col32
,col33
,col34
,col35
,col36
,col37
,col38
,COALESCE(trim(col39),'N') AS col39
,COALESCE(Parent_col9,-999) AS Parent_col9
,COALESCE(col40,-1) AS col40
,col41
,col42
,CASE WHEN trim(col43)='' THEN 'U' ELSE COALESCE(trim(col43),'U') END AS col43
,col44
,col45
,col46
,col47
,col48
,col49
,col50
,col51
,col12
,col52
,col53
,col54
,col55
,col56
,col57
,COALESCE(col58,-1) AS col58
,col59
,col60
,col61
,col62
,(CASE WHEN trim(cast(col63 as string))='' THEN '-1' ELSE COALESCE(trim(cast(col63 as string)),'-1')END) AS col63
,col64
,col65
,col66
,COALESCE(col67,-999) as col67
,col68
,col69
,col70
,col71
,col72
,col73
,col74
,col75
,col76
,col77
,col116
,col117
,COALESCE(col78,-1) AS col78
,col79 as col79
,col114
,col115 
,COALESCE(col92,'-99') AS col92
,COALESCE(col80,-1) AS col80
,col81
,col93
,col82 as col82
,col83
,col89 AS col118
,COALESCE(col84,'-99') AS col84
,col85
,col86
,col87
,COALESCE(col88,'-99') AS col88
FROM cpn.mytable_1000_n;

CREATE OR REPLACE VIEW cpn_view.myview_1000_n AS
SELECT
col0
,col1
,col2
,col3
,col4
,col5
,col6
,col7
,COALESCE(col8,0.0) AS col8
,COALESCE(col9,0) as col9
,col10
,CASE WHEN COALESCE(col11,'$')='X' THEN 'Y' ELSE col11 END as Alt_col11
,CASE WHEN COALESCE(col12,'$')='X' THEN 'Y' ELSE col12 END as Alt_col12
,col13
,(Case WHEN trim(col5) IN ('A','B','C') THEN COALESCE(col14,-1) ELSE COALESCE(col14,1) END) AS col14
,col15
,col16
,col17
,col18
,col19
,col20
,col21
,col22
,col23
,COALESCE(trim(col11),'Z') AS col11
,col24
,col25
,col26
,col27
,col28
,col29
,col30
,col31
,col32
,col33
,col34
,col35
,col36
,col37
,col38
,COALESCE(trim(col39),'N') AS col39
,COALESCE(Parent_col9,-999) AS Parent_col9
,COALESCE(col40,-1) AS col40
,col41
,col42
,CASE WHEN trim(col43)='' THEN 'U' ELSE COALESCE(trim(col43),'U') END AS col43
,col44
,col45
,col46
,col47
,col48
,col49
,col50
,col51
,col12
,col52
,col53
,col54
,col55
,col56
,col57
,COALESCE(col58,-1) AS col58
,col59
,col60
,col61
,col62
,(CASE WHEN trim(cast(col63 as string))='' THEN '-1' ELSE COALESCE(trim(cast(col63 as string)),'-1')END) AS col63
,col64
,col65
,col66
,COALESCE(col67,-999) as col67
,col68
,col69
,col70
,col71
,col72
,col73
,col74
,col75
,col76
,col77
,col116
,col117
,COALESCE(col78,-1) AS col78
,col79 as col79
,col114
,col115 
,COALESCE(col92,'-99') AS col92
,COALESCE(col80,-1) AS col80
,col81
,col93
,col82 as col82
,col83
,col89 AS col118
,COALESCE(col84,'-99') AS col84
,col85
,col86
,col87
,COALESCE(col88,'-99') AS col88
FROM cpn.mytable_1000_n;

select * from cpn_view.myview_1000_n;

drop view cpn_view.myview_1000_n;
drop table cpn.mytable_1000_n;
