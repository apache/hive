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
package org.apache.hadoop.hive.metastore.tools.schematool;

import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper.MetaStoreConnectionInfo;

/**
 * Print Hive version and schema version.
 */
class SchemaToolTaskInfo extends SchemaToolTask {
  @Override
  void setCommandLineArguments(SchemaToolCommandLine cl) {
    // do nothing
  }

  @Override
  void execute() throws HiveMetaException {
    String hiveVersion = schemaTool.getMetaStoreSchemaInfo().getHiveSchemaVersion();
    MetaStoreConnectionInfo connectionInfo = schemaTool.getConnectionInfo(true);
    String dbVersion = schemaTool.getMetaStoreSchemaInfo().getMetaStoreSchemaVersion(connectionInfo);

    System.out.println("Hive distribution version:\t " + hiveVersion);
    System.out.println("Metastore schema version:\t " + dbVersion);

    schemaTool.assertCompatibleVersion(hiveVersion, dbVersion);
  }
}
