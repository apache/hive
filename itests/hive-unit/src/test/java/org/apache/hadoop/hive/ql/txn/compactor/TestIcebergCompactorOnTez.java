/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hive.ql.txn.compactor.TestCompactor.executeStatementOnDriver;

public class TestIcebergCompactorOnTez extends CompactorOnTezTest {

  @Test
  public void testIcebergCompactorWithAllPartitionFieldTypes() throws Exception{
    conf.setVar(HiveConf.ConfVars.COMPACTOR_JOB_QUEUE, CUSTOM_COMPACTION_QUEUE);
    msClient = new HiveMetaStoreClient(conf);

    String dbName = "default";
    String tableName = "ice_orc";
    String qualifiedTableName = dbName + "." + tableName;

    executeStatementOnDriver("drop table if exists " + qualifiedTableName, driver);
    executeStatementOnDriver(String.format("create table %s " +
        "(id int, a string, b int, c bigint, d float, e double, f decimal(4, 2), g boolean, h date, i date, j date, k timestamp) " +
        "partitioned by spec(a, truncate(3, a), bucket(4, a), b, c, d, e, f, g, h, year(h), month(i), day(j), k, hour(k)) stored by iceberg stored as orc " +
        "tblproperties ('compactor.threshold.min.input.files'='1')", qualifiedTableName), driver);

    // 6 records, one records per file --> 3 partitions, 2 files per partition
    executeStatementOnDriver(String.format("INSERT INTO %s VALUES (1, 'aaa111', 1, 100, 1.0, 2.0, 4.00, true,  DATE '2024-05-01', DATE '2024-05-01', DATE '2024-05-01', TIMESTAMP '2024-05-02 10:00:00')", qualifiedTableName), driver);
    executeStatementOnDriver(String.format("INSERT INTO %s VALUES (2, 'aaa111', 1, 100, 1.0, 2.0, 4.00, true,  DATE '2024-05-01', DATE '2024-05-01', DATE '2024-05-01', TIMESTAMP '2024-05-02 10:00:00')", qualifiedTableName), driver);
    executeStatementOnDriver(String.format("INSERT INTO %s VALUES (3, 'bbb222', 2, 200, 2.0, 3.0, 8.00, false, DATE '2024-05-03', DATE '2024-05-03', DATE '2024-05-03', TIMESTAMP '2024-05-04 13:00:00')", qualifiedTableName), driver);
    executeStatementOnDriver(String.format("INSERT INTO %s VALUES (4, 'bbb222', 2, 200, 2.0, 3.0, 8.00, false, DATE '2024-05-03', DATE '2024-05-03', DATE '2024-05-03', TIMESTAMP '2024-05-04 13:00:00')", qualifiedTableName), driver);
    executeStatementOnDriver(String.format("INSERT INTO %s VALUES (5, null, null, null, null, null, null, null, null, null, null, null)", qualifiedTableName), driver);
    executeStatementOnDriver(String.format("INSERT INTO %s VALUES (6, null, null, null, null, null, null, null, null, null, null, null)", qualifiedTableName), driver);

    Assert.assertEquals(6, getFilesCount(qualifiedTableName));
    List<String> recordsBefore = getAllRecords(qualifiedTableName);

    CompactorTestUtil.runCompaction(conf, dbName, tableName, CompactionType.MINOR, false, 
        "a=aaa111/a_trunc=aaa/a_bucket=0/b=1/c=100/d=1.0/e=2.0/f=4.00/g=true/h=2024-05-01/h_year=2024/i_month=2024-05/j_day=2024-05-01/k=2024-05-02T10%3A00%3A00/k_hour=2024-05-02-10",
        "a=bbb222/a_trunc=bbb/a_bucket=3/b=2/c=200/d=2.0/e=3.0/f=8.00/g=false/h=2024-05-03/h_year=2024/i_month=2024-05/j_day=2024-05-03/k=2024-05-04T13%3A00%3A00/k_hour=2024-05-04-13",
        "a=null/a_trunc=null/a_bucket=null/b=null/c=null/d=null/e=null/f=null/g=null/h=null/h_year=null/i_month=null/j_day=null/k=null/k_hour=null"
    );
    
    Assert.assertEquals(3, getFilesCount(qualifiedTableName));
    verifySuccessfulCompaction(3);
    List<String> recordsAfter = getAllRecords(qualifiedTableName);
    
    Assert.assertEquals(recordsBefore, recordsAfter);
  }
  
  private int getFilesCount(String qualifiedTableName) throws Exception {
    driver.run(String.format("select count(*) from %s.files", qualifiedTableName));
    List<String> res = new ArrayList<>();
    driver.getFetchTask().fetch(res);
    return Integer.parseInt(res.get(0));
  }

  private List<String> getAllRecords(String qualifiedTableName) throws Exception {
    driver.run(String.format("select * from %s order by id", qualifiedTableName));
    List<String> res = new ArrayList<>();
    driver.getFetchTask().fetch(res);
    return res;
  }
}
