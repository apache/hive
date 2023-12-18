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
package org.apache.hadoop.hive.metastore.tools.schematool.hms;

import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.SchemaInfo;
import org.apache.hadoop.hive.metastore.tools.schematool.task.SchemaToolTask;
import org.apache.hadoop.hive.metastore.tools.schematool.task.TaskContext;

import java.util.Set;

/**
 * Print Hive version and schema version.
 */
class SchemaToolTaskInfo extends SchemaToolTask {

  @Override
  protected Set<String> usedCommandLineArguments() {
    return null;
  }

  @Override
  protected void execute(TaskContext context) throws HiveMetaException {
    SchemaInfo schemaInfo = context.getSchemaInfo();
    String minimumRequiredVersion = SchemaInfo.getRequiredHmsSchemaVersion();
    String dbVersion = schemaInfo.getSchemaVersion();

    System.out.println("Hive distribution version:\t " + minimumRequiredVersion);
    System.out.println("Schema version:\t " + dbVersion);

    if (!SchemaInfo.isVersionCompatible(minimumRequiredVersion, dbVersion)) {
      throw new HiveMetaException("The HMS schema version (" + dbVersion +
          ") is not compatible with the minimum required version (" + minimumRequiredVersion + ")");
    }
  }

}
