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
package org.apache.hadoop.hive.ql.ddl;

import java.util.Collections;

import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.junit.Test;

import static org.junit.Assert.fail;

public class TestJdbcStorageHandlerDdlUtils {

  @Test
  public void testRejectMetastoreTypeOutsideSys() throws Exception {
    HiveConf conf = new HiveConf();
    try {
      JdbcStorageHandlerDdlUtils.validateMetastoreDatabaseTypeProperty(conf,
          "org.apache.hive.storage.jdbc.JdbcStorageHandler", "attacker_db",
          Collections.singletonMap(Constants.JDBC_DATABASE_TYPE, "METASTORE"));
      fail("Expected SemanticException");
    } catch (SemanticException expected) {
      // expected
    }
  }

  @Test
  public void testAllowMetastoreTypeInSys() throws Exception {
    HiveConf conf = new HiveConf();
    JdbcStorageHandlerDdlUtils.validateMetastoreDatabaseTypeProperty(conf,
        "org.apache.hive.storage.jdbc.JdbcStorageHandler", "sys",
        Collections.singletonMap(Constants.JDBC_DATABASE_TYPE, "METASTORE"));
  }
}
