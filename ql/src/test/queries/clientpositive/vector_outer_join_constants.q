set hive.tez.container.size = 8192;
set hive.stats.fetch.column.stats=true;
set hive.cbo.enable=true;
set hive.tez.dynamic.partition.pruning=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.optimize.metadataonly=true;
set hive.optimize.reducededuplication=true;
set hive.optimize.null.scan=true;
set hive.mapjoin.optimized.hashtable=true;
set hive.optimize.constant.propagation=true;
set hive.optimize.index.filter=true;
set hive.optimize.bucketmapjoin=true;
set hive.limit.optimize.enable=true;
set hive.optimize.bucketmapjoin.sortedmerge=false;
set hive.optimize.reducededuplication.min.reducer=1;
set hive.vectorized.execution.reduce.enabled=true;
set hive.auto.convert.join=true;
set hive.vectorized.execution.mapjoin.native.enabled=true;
set hive.optimize.dynamic.partition.hashjoin=true;
set hive.tez.bucket.pruning=true;

set hive.vectorized.execution.enabled=true;

-- set hive.optimize.dynamic.partition.hashjoin=false;

CREATE EXTERNAL TABLE item(ID int, S_ID int, NAME string);

CREATE EXTERNAL TABLE odetail(ID int, O_DATE timestamp);

CREATE EXTERNAL TABLE ytday(D_DATE timestamp, YTD_DATE timestamp );

CREATE EXTERNAL TABLE lday(D_DATE timestamp, LY_DATE timestamp);


INSERT INTO item values(101, 22, "Item 101");
INSERT INTO item values(102, 22, "Item 102");

INSERT INTO odetail values(101, '2001-06-30 00:00:00');
INSERT INTO odetail values(102, '2002-06-30 00:00:00');

INSERT INTO ytday values('2008-04-30 00:00:00', '2001-06-30 00:00:00');
INSERT INTO ytday values('2008-04-30 00:00:00', '2022-06-30 00:00:00');

INSERT INTO lday values('2021-06-30 00:00:00', '2001-06-30 00:00:00');
INSERT INTO lday values('2022-06-30 00:00:00', '2002-06-30 00:00:00');

analyze table item compute statistics;
analyze table item compute statistics for columns;

analyze table odetail compute statistics;
analyze table odetail compute statistics for columns;

analyze table ytday compute statistics;
analyze table ytday compute statistics for columns;

analyze table lday compute statistics;
analyze table lday compute statistics for columns;

EXPLAIN VECTORIZATION DETAIL
select * from
(select        item1.S_ID  S_ID,
                ytday1.D_DATE  D_DATE
        from        odetail        od1
                join        ytday        ytday1
                  on         (od1.O_DATE = ytday1.YTD_DATE)
                join        item        item1
                  on         (od1.ID = item1.ID)
        where        (item1.S_ID in (22)
         and ytday1.D_DATE = '2008-04-30 00:00:00')
        group by        item1.S_ID,
                ytday1.D_DATE
        )        pa11
        full outer join
        (select        item2.S_ID  S_ID,
                ytday2.D_DATE  D_DATE
        from        odetail        od2
                join        lday        lday2 -- map8
                  on         (od2.O_DATE = lday2.LY_DATE)
                join        ytday        ytday2
                  on         (lday2.D_DATE = ytday2.YTD_DATE)
                join        item        item2
                  on         (od2.ID = item2.ID)
        where        (item2.S_ID in (22)
         and ytday2.D_DATE = '2008-04-30 00:00:00')
        group by        item2.S_ID,
                ytday2.D_DATE
        )        pa12
          on         (pa11.D_DATE = pa12.D_DATE and
        pa11.S_ID = pa12.S_ID);

select * from
(select        item1.S_ID  S_ID,
                ytday1.D_DATE  D_DATE
        from        odetail        od1
                join        ytday        ytday1
                  on         (od1.O_DATE = ytday1.YTD_DATE)
                join        item        item1
                  on         (od1.ID = item1.ID)
        where        (item1.S_ID in (22)
         and ytday1.D_DATE = '2008-04-30 00:00:00')
        group by        item1.S_ID,
                ytday1.D_DATE
        )        pa11
        full outer join
        (select        item2.S_ID  S_ID,
                ytday2.D_DATE  D_DATE
        from        odetail        od2
                join        lday        lday2 -- map8
                  on         (od2.O_DATE = lday2.LY_DATE)
                join        ytday        ytday2
                  on         (lday2.D_DATE = ytday2.YTD_DATE)
                join        item        item2
                  on         (od2.ID = item2.ID)
        where        (item2.S_ID in (22)
         and ytday2.D_DATE = '2008-04-30 00:00:00')
        group by        item2.S_ID,
                ytday2.D_DATE
        )        pa12
          on         (pa11.D_DATE = pa12.D_DATE and
        pa11.S_ID = pa12.S_ID);
