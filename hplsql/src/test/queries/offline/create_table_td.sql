CREATE TABLE tab, NO LOG, NO FALLBACK
  (
    SOURCE_ID  INT,
    RUN_ID     INT,
    STATUS     CHAR,
    LOAD_START timestamp(0),
    LOAD_END   timestamp(0)
  );

CREATE TABLE ctl, NO LOG, NO FALLBACK
AS
(
        SELECT
                EBC.SOURCE_ID,
                MAX(EBC.RUN_ID) AS RUN_ID,
                EBC.STATUS,
                EBC.LOAD_START,
                EBC.LOAD_END
        FROM
                EBC
        WHERE
                EBC.SOURCE_ID = 451 AND
                EBC.STATUS = 'R'
        GROUP BY
                1,3,4,5
);
  
CREATE SET VOLATILE TABLE ctl2, NO LOG, NO FALLBACK
AS
(
        SELECT
                EBC.SOURCE_ID,
                MAX(EBC.RUN_ID) AS RUN_ID,
                EBC.STATUS,
                EBC.LOAD_START,
                EBC.LOAD_END
        FROM
                EBC
        WHERE
                EBC.SOURCE_ID = 451 AND
                EBC.STATUS = 'R'
        GROUP BY
                1,3,4,5
) WITH DATA PRIMARY INDEX (LOAD_START,LOAD_END)
  ON COMMIT PRESERVE ROWS ;