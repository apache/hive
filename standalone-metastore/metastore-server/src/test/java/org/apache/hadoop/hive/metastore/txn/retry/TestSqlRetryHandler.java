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
package org.apache.hadoop.hive.metastore.txn.retry;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;

public class TestSqlRetryHandler {

  @Test
  public void testRetryableRegex() {
    HiveConf conf = new HiveConf();
    SQLException sqlException = new SQLException("ORA-08177: can't serialize access for this transaction", "72000");
    // Note that we have 3 regex'es below
    conf.setVar(HiveConf.ConfVars.HIVE_TXN_RETRYABLE_SQLEX_REGEX, "^Deadlock detected, roll back,.*08177.*,.*08178.*");
    boolean result = SqlRetryHandler.isRetryable(conf, sqlException);
    Assert.assertTrue("regex should be retryable", result);

    sqlException = new SQLException("This error message, has comma in it");
    conf.setVar(HiveConf.ConfVars.HIVE_TXN_RETRYABLE_SQLEX_REGEX, ".*comma.*");
    result = SqlRetryHandler.isRetryable(conf, sqlException);
    Assert.assertTrue("regex should be retryable", result);
  }
  
}
