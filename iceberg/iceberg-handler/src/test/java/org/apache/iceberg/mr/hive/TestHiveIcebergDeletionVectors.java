/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive;

import java.util.Collection;
import java.util.List;
import java.util.stream.LongStream;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.mr.hive.test.TestTables.TestTableType;
import org.apache.iceberg.mr.hive.test.utils.HiveIcebergStorageHandlerTestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

/**
 * Row-level DML on V3 tables: the deletion vectors of the data files.
 */
public class TestHiveIcebergDeletionVectors extends HiveIcebergStorageHandlerWithEngineBase {

  private static final int ROWS = 1000;

  @Parameters(name = "fileFormat={0}, catalog={1}, isVectorized={2}, formatVersion={3}")
  public static Collection<Object[]> parameters() {
    return getParameters(p -> p.fileFormat() == FileFormat.PARQUET &&
        p.testTableType() == TestTableType.HIVE_CATALOG && p.formatVersion() == 3);
  }

  private List<Record> records(long from, long to) {
    List<Record> records = TestHelper.generateRandomRecords(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        (int) (to - from), from);
    for (int i = 0; i < records.size(); i++) {
      records.get(i).setField("customer_id", from + i);
    }
    return records;
  }

  private long count(String sql) {
    return (Long) shell.executeStatement(sql).get(0)[0];
  }

  /**
   * Main and the branch each hold a DV the other lacks: a DML into the branch merges only the branch's.
   */
  @Test
  public void testDmlIntoBranchMergesTheBranchDVs() {
    testTables.createTable(shell, "dv_branch", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, records(0, ROWS), formatVersion);
    shell.executeStatement("ALTER TABLE dv_branch CREATE BRANCH b1");
    shell.executeStatement("CREATE TABLE dv_merge_src STORED BY ICEBERG AS " +
        "SELECT customer_id FROM dv_branch WHERE customer_id % 7 = 0");
    shell.executeStatement("DELETE FROM dv_branch WHERE customer_id % 3 = 0");
    shell.executeStatement("DELETE FROM default.dv_branch.branch_b1 WHERE customer_id % 5 = 0");
    shell.executeStatement("MERGE INTO default.dv_branch.branch_b1 t USING dv_merge_src s " +
        "ON t.customer_id = s.customer_id WHEN MATCHED THEN DELETE");
    long deleted = LongStream.range(0, ROWS).filter(i -> i % 7 == 0 || i % 5 == 0).count();
    Assert.assertEquals(ROWS - deleted, count("SELECT count(*) FROM default.dv_branch.branch_b1"));
  }
}
