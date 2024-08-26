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

package org.apache.hadoop.hive.ql.qoption;

import org.apache.hadoop.hive.metastore.MetaStoreSchemaInfoFactory;
import org.apache.hadoop.hive.ql.QTestUtil;
import org.apache.hive.testutils.HiveTestEnvSetup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * QTest sysdb directive handler
 *
 * Loads the sysdb schema prior to running the test.
 *
 * Example:
 * --! qt:sysdb
 * 
 */
public class QTestSysDbHandler implements QTestOptionHandler {
  private static final Logger LOG = LoggerFactory.getLogger(QTestSysDbHandler.class.getName());
  private boolean enabled;

  @Override
  public void processArguments(String arguments) {
    enabled = true;
  }

  @Override
  public void beforeTest(QTestUtil qt) throws Exception {
    if (enabled) {
      String schemaVersion = MetaStoreSchemaInfoFactory.get(qt.getConf()).getHiveSchemaVersion();
      String stsdbPath =
          HiveTestEnvSetup.HIVE_ROOT + "/standalone-metastore/metastore-server/src/main/sql/hive/hive-schema-" + schemaVersion + ".hive.sql";
      qt.getCliDriver().processLine("source " + stsdbPath);
      qt.getCliDriver().processLine("use default");
    }
  }

  @Override
  public void afterTest(QTestUtil qt) throws Exception {
    enabled = false;
  }

}
