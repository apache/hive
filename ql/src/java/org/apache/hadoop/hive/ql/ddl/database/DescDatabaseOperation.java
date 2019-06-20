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

package org.apache.hadoop.hive.ql.ddl.database;

import java.io.DataOutputStream;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Operation process of describing a database.
 */
public class DescDatabaseOperation extends DDLOperation<DescDatabaseDesc> {
  public DescDatabaseOperation(DDLOperationContext context, DescDatabaseDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    try (DataOutputStream outStream = DDLUtils.getOutputStream(new Path(desc.getResFile()), context)) {
      Database database = context.getDb().getDatabase(desc.getDatabaseName());
      if (database == null) {
        throw new HiveException(ErrorMsg.DATABASE_NOT_EXISTS, desc.getDatabaseName());
      }

      SortedMap<String, String> params = null;
      if (desc.isExt()) {
        params = new TreeMap<>(database.getParameters());
      }

      String location = database.getLocationUri();
      if (HiveConf.getBoolVar(context.getConf(), HiveConf.ConfVars.HIVE_IN_TEST)) {
        location = "location/in/test";
      }

      context.getFormatter().showDatabaseDescription(outStream, database.getName(), database.getDescription(),
          location, database.getOwnerName(), database.getOwnerType(), params);
    } catch (Exception e) {
      throw new HiveException(e, ErrorMsg.GENERIC_ERROR);
    }

    return 0;
  }
}
