/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.txn.compactor;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.hadoop.hive.ql.txn.compactor.TestCompactor.executeStatementOnDriver;
import static org.apache.hadoop.hive.ql.txn.compactor.TestCompactor.execSelectAndDumpData;
import static org.apache.hadoop.hive.ql.txn.compactor.TestCompactor.executeStatementOnDriverSilently;

public class TestFetchWriteIdFromInsertOnlyTables extends CompactorOnTezTest {

  private static final String TABLE1 = "t1";

  private static final List<String> EXPECTED_RESULT = Arrays.asList(
      "0\t10\t10",
      "0\t1\t1",
      "0\t2\t20",
      "3\t2\t32",
      "3\t10\t15",
      "3\t42\t42"
  );


  @Override
  public void setup() throws Exception {
    super.setup();

    executeStatementOnDriver(
        "create table " + TABLE1 + "(a int, b int) " +
            "stored as orc TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only')", driver);
    executeStatementOnDriver("insert into " + TABLE1 + "(a,b) values (1, 1), (10, 10)", driver);
    executeStatementOnDriver("insert into " + TABLE1 + "(a,b) values (2, 20)", driver);
  }

  @Override
  public void tearDown() {
    executeStatementOnDriverSilently("drop table " + TABLE1 , driver);

    super.tearDown();
  }

  @Test
  public void testFetchWriteIdAfterCompaction() throws Exception {

    CompactorTestUtil.runCompaction(conf, "default",  TABLE1 , CompactionType.MAJOR, true);
    CompactorTestUtil.runCleaner(conf);
    verifySuccessfulCompaction(1);

    executeStatementOnDriver("insert into " + TABLE1 + "(a,b) values (10, 15), (2, 32), (42, 42)", driver);

    List<String>  result = execSelectAndDumpData(
        "SELECT t1.ROW__ID.writeId, a, b FROM " + TABLE1 + "('insertonly.fetch.bucketid'='true')" , driver, "");
    assertResult(EXPECTED_RESULT, result);
  }

  private void assertResult(List<String> expected, List<String> actual) {
    Assert.assertEquals(expected.size(), actual.size());

    Collections.sort(expected);
    Collections.sort(actual);
    Assert.assertEquals(expected, actual);
  }

}
