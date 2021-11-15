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

import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.junit.Before;

import java.sql.SQLException;

/**
 * Test for cleaner to run when min_history_level is dropped and it has to fallback to not using the table.
 * HMS can seamlessly switch to the new functionality every time it tries to access the table and it is not there anymore.
 * This can help upgrade procedure, where the table can be dropped as the last step, after every HMS has been updated.
 */
public class TestCleanerWithoutMinHistoryLevel extends TestCleaner {

  @Before
  public void dropMinHistoryLevel() throws Exception {
    try {
      TestTxnDbUtil.executeUpdate(conf, "DROP TABLE MIN_HISTORY_LEVEL");
    } catch (Exception e) {
      if (e instanceof SQLException) {
        SQLException ex = (SQLException) e;
        if (ex.getSQLState().equalsIgnoreCase("42Y55")) {
          // It was already dropped
          return;
        }
      }
      throw e;
    }
  }
}
