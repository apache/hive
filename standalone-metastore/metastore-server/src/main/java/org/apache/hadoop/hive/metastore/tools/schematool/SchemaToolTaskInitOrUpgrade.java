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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Perform metastore schema init or upgrade based on schema version
 */

public class SchemaToolTaskInitOrUpgrade extends SchemaToolTask {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaToolTaskInitOrUpgrade.class);
  private SchemaToolCommandLine cl;
  @Override
  void setCommandLineArguments(SchemaToolCommandLine cl) {
    this.cl = cl;
  }

  @Override
  void execute() throws HiveMetaException {
    HiveSchemaHelper.MetaStoreConnectionInfo connectionInfo = schemaTool.getConnectionInfo(true);
    String dbVersion = null;
    try {
      dbVersion = schemaTool.getMetaStoreSchemaInfo().getMetaStoreSchemaVersion(connectionInfo);
    } catch (HiveMetaException e) {
      LOG.info("Exception getting db version:" + e.getMessage());
      LOG.info("Try to initialize db schema");
    }
    SchemaToolTask task;
    if (dbVersion == null) {
      task = new SchemaToolTaskInit();
    } else {
      task = new SchemaToolTaskUpgrade();
    }
    task.setHiveSchemaTool(schemaTool);
    task.setCommandLineArguments(cl);
    task.execute();
  }
}