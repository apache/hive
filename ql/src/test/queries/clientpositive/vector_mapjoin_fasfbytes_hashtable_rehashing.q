create table tbl1_mst
(
    ID                          String,
    OWNERID                     String,
    ISDELETED                   String,
    NAME                        String,
    RECORDTYPEID                String,
    CREATEDDATE                 String,
    LASTMODIFIEDDATE            String,
    fld1                        String,
    fld2                String,
    fld3 String,
    fld4 String,
    fld5            String,
    fld6               String,
    fld7                  String,
    fld8                String,
    fld9             String,
    fld10               String,
    fld11             String,
    fld12               String,
    fld13                     String,
    fld14             String,
    fld15               String,
    fld16   String,
    fld17   String,
    fld18            String,
    fld19           String,
    fld20            String,
    fld21                String,
    fld22          String,
    fld23         String,
    fld24               String,
    fld25                     String,
    fld26 String,
    fld27            String,
    fld28           String,
    fld29          String,
    fld30  String,
    fld31               String,
    fld32         String,
    fld33               String
)
    row format delimited fields terminated by "\t"
;

LOAD DATA LOCAL INPATH '../../data/files/engesc8151.txt' OVERWRITE INTO TABLE tbl1_mst;

create table tbl1_mst2 row format delimited fields terminated by "\t" as
select ID
     , OWNERID
     , ISDELETED
     , NAME
     , RECORDTYPEID
     , CREATEDDATE
     , LASTMODIFIEDDATE
     , fld1
     , fld2
     , fld3
     , fld4
     , fld5
     , fld6
     , fld7
     , fld8
     , fld9
     , fld10
     , fld11
     , fld12
     , fld13
     , fld14
     , fld15
     , fld16
     , fld17
     , fld18
     , fld19
     , fld20
     , fld21
     , fld22
     , fld23
     , fld24
     , fld25
     , fld26
     , fld27
     , fld28
     , fld29
     , fld30
     , fld31
     , fld32
     , fld33
from tbl1_mst
;

set hive.auto.convert.join=true;
set hive.vectorized.execution.mapjoin.native.fast.hashtable.enabled=true;
set hive.hashtable.loadfactor=1;
set hive.hashtable.key.count.adjustment=0;
set hive.hashtable.initialCapacity=1;

SELECT *
FROM tbl1_mst T1
WHERE NOT EXISTS (SELECT *
                  FROM tbl1_mst2 T2
                  WHERE NVL(T1.id, '') = NVL(T2.id, '')
                    AND NVL(T1.ownerid, '') = NVL(T2.ownerid, '')
                    AND NVL(T1.isdeleted, '') = NVL(T2.isdeleted, '')
                    AND NVL(T1.name, '') = NVL(T2.name, '')
                    AND NVL(T1.recordtypeid, '') = NVL(T2.recordtypeid, '')
                    AND NVL(T1.createddate, '') = NVL(T2.createddate, '')
                    AND NVL(T1.lastmodifieddate, '') = NVL(T2.lastmodifieddate, '')
                    AND NVL(T1.fld1, '') = NVL(T2.fld1, '')
                    AND NVL(T1.fld2, '') = NVL(T2.fld2, '')
                    AND NVL(T1.fld3, '') = NVL(T2.fld3, '')
                    AND NVL(T1.fld4, '') = NVL(T2.fld4, '')
                    AND NVL(T1.fld5, '') = NVL(T2.fld5, '')
                    AND NVL(T1.fld6, '') = NVL(T2.fld6, '')
                    AND NVL(T1.fld7, '') = NVL(T2.fld7, '')
                    AND NVL(T1.fld8, '') = NVL(T2.fld8, '')
                    AND NVL(T1.fld9, '') = NVL(T2.fld9, '')
                    AND NVL(T1.fld10, '') = NVL(T2.fld10, '')
                    AND NVL(T1.fld11, '') = NVL(T2.fld11, '')
                    AND NVL(T1.fld12, '') = NVL(T2.fld12, '')
                    AND NVL(T1.fld13, '') = NVL(T2.fld13, '')
                    AND NVL(T1.fld14, '') = NVL(T2.fld14, '')
                    AND NVL(T1.fld15, '') = NVL(T2.fld15, '')
                    AND NVL(T1.fld16, '') = NVL(T2.fld16, '')
                    AND NVL(T1.fld17, '') = NVL(T2.fld17, '')
                    AND NVL(T1.fld18, '') = NVL(T2.fld18, '')
                    AND NVL(T1.fld19, '') = NVL(T2.fld19, '')
                    AND NVL(T1.fld20, '') = NVL(T2.fld20, '')
                    AND NVL(T1.fld21, '') = NVL(T2.fld21, '')
                    AND NVL(T1.fld22, '') = NVL(T2.fld22, '')
                    AND NVL(T1.fld23, '') = NVL(T2.fld23, '')
                    AND NVL(T1.fld24, '') = NVL(T2.fld24, '')
                    AND NVL(T1.fld25, '') = NVL(T2.fld25, '')
                    AND NVL(T1.fld26, '') = NVL(T2.fld26, '')
                    AND NVL(T1.fld27, '') = NVL(T2.fld27, '')
                    AND NVL(T1.fld28, '') = NVL(T2.fld28, '')
                    AND NVL(T1.fld29, '') = NVL(T2.fld29, '')
                    AND NVL(T1.fld30, '') = NVL(T2.fld30, '')
                    AND NVL(T1.fld31, '') = NVL(T2.fld31, '')
                    AND NVL(T1.fld32, '') = NVL(T2.fld32, '')
                    AND NVL(T1.fld33, '') = NVL(T2.fld33, '')
);

SELECT *
FROM tbl1_mst T1
WHERE EXISTS (SELECT *
                  FROM tbl1_mst2 T2
                  WHERE NVL(T1.id, '') = NVL(T2.id, '')
                    AND NVL(T1.ownerid, '') = NVL(T2.ownerid, '')
                    AND NVL(T1.isdeleted, '') = NVL(T2.isdeleted, '')
                    AND NVL(T1.name, '') = NVL(T2.name, '')
                    AND NVL(T1.recordtypeid, '') = NVL(T2.recordtypeid, '')
                    AND NVL(T1.createddate, '') = NVL(T2.createddate, '')
                    AND NVL(T1.lastmodifieddate, '') = NVL(T2.lastmodifieddate, '')
                    AND NVL(T1.fld1, '') = NVL(T2.fld1, '')
                    AND NVL(T1.fld2, '') = NVL(T2.fld2, '')
                    AND NVL(T1.fld3, '') = NVL(T2.fld3, '')
                    AND NVL(T1.fld4, '') = NVL(T2.fld4, '')
                    AND NVL(T1.fld5, '') = NVL(T2.fld5, '')
                    AND NVL(T1.fld6, '') = NVL(T2.fld6, '')
                    AND NVL(T1.fld7, '') = NVL(T2.fld7, '')
                    AND NVL(T1.fld8, '') = NVL(T2.fld8, '')
                    AND NVL(T1.fld9, '') = NVL(T2.fld9, '')
                    AND NVL(T1.fld10, '') = NVL(T2.fld10, '')
                    AND NVL(T1.fld11, '') = NVL(T2.fld11, '')
                    AND NVL(T1.fld12, '') = NVL(T2.fld12, '')
                    AND NVL(T1.fld13, '') = NVL(T2.fld13, '')
                    AND NVL(T1.fld14, '') = NVL(T2.fld14, '')
                    AND NVL(T1.fld15, '') = NVL(T2.fld15, '')
                    AND NVL(T1.fld16, '') = NVL(T2.fld16, '')
                    AND NVL(T1.fld17, '') = NVL(T2.fld17, '')
                    AND NVL(T1.fld18, '') = NVL(T2.fld18, '')
                    AND NVL(T1.fld19, '') = NVL(T2.fld19, '')
                    AND NVL(T1.fld20, '') = NVL(T2.fld20, '')
                    AND NVL(T1.fld21, '') = NVL(T2.fld21, '')
                    AND NVL(T1.fld22, '') = NVL(T2.fld22, '')
                    AND NVL(T1.fld23, '') = NVL(T2.fld23, '')
                    AND NVL(T1.fld24, '') = NVL(T2.fld24, '')
                    AND NVL(T1.fld25, '') = NVL(T2.fld25, '')
                    AND NVL(T1.fld26, '') = NVL(T2.fld26, '')
                    AND NVL(T1.fld27, '') = NVL(T2.fld27, '')
                    AND NVL(T1.fld28, '') = NVL(T2.fld28, '')
                    AND NVL(T1.fld29, '') = NVL(T2.fld29, '')
                    AND NVL(T1.fld30, '') = NVL(T2.fld30, '')
                    AND NVL(T1.fld31, '') = NVL(T2.fld31, '')
                    AND NVL(T1.fld32, '') = NVL(T2.fld32, '')
                    AND NVL(T1.fld33, '') = NVL(T2.fld33, '')
);