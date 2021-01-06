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

package org.apache.hadoop.hive.ql.ddl.database.showcreate;

import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.ShowUtils;

import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.IOUtils;
import org.apache.hive.common.util.HiveStringUtils;

/**
 * Operation process showing the creation of a database.
 */
public class ShowCreateDatabaseOperation extends DDLOperation<ShowCreateDatabaseDesc> {
  public ShowCreateDatabaseOperation(DDLOperationContext context, ShowCreateDatabaseDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    DataOutputStream outStream = ShowUtils.getOutputStream(desc.getResFile(), context);
    try {
      return showCreateDatabase(outStream);
    } catch (Exception e) {
      throw new HiveException(e);
    } finally {
      IOUtils.closeStream(outStream);
    }
  }

  private int showCreateDatabase(DataOutputStream outStream) throws Exception {
    Database database = context.getDb().getDatabase(desc.getDatabaseName());

    StringBuilder createDbCommand = new StringBuilder();
    createDbCommand.append("CREATE DATABASE `").append(database.getName()).append("`\n");
    if (database.getDescription() != null) {
      createDbCommand.append("COMMENT\n  '");
      createDbCommand.append(HiveStringUtils.escapeHiveCommand(database.getDescription())).append("'\n");
    }
    createDbCommand.append("LOCATION\n  '");
    createDbCommand.append(database.getLocationUri()).append("'\n");
    if (database.getManagedLocationUri() != null) {
      createDbCommand.append("MANAGEDLOCATION\n  '");
      createDbCommand.append(database.getManagedLocationUri()).append("'\n");
    }
    String propertiesToString = ShowUtils.propertiesToString(database.getParameters(), Collections.emptySet());
    if (!propertiesToString.isEmpty()) {
      createDbCommand.append("WITH DBPROPERTIES (\n");
      createDbCommand.append(propertiesToString).append(")\n");
    }

    outStream.write(createDbCommand.toString().getBytes(StandardCharsets.UTF_8));
    return 0;
  }
}
