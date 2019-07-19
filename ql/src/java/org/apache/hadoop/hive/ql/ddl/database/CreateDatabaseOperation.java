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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * Operation process of creating a database.
 */
public class CreateDatabaseOperation extends DDLOperation<CreateDatabaseDesc> {
  private static final String DATABASE_PATH_SUFFIX = ".db";

  public CreateDatabaseOperation(DDLOperationContext context, CreateDatabaseDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    Database database = new Database();
    database.setName(desc.getName());
    database.setDescription(desc.getComment());
    database.setLocationUri(desc.getLocationUri());
    database.setParameters(desc.getDatabaseProperties());
    database.setOwnerName(SessionState.getUserFromAuthenticator());
    database.setOwnerType(PrincipalType.USER);

    try {
      makeLocationQualified(database);
      context.getDb().createDatabase(database, desc.getIfNotExists());
    } catch (AlreadyExistsException ex) {
      //it would be better if AlreadyExistsException had an errorCode field....
      throw new HiveException(ex, ErrorMsg.DATABASE_ALREADY_EXISTS, desc.getName());
    }

    return 0;
  }

  private void makeLocationQualified(Database database) throws HiveException {
    if (database.isSetLocationUri()) {
      database.setLocationUri(Utilities.getQualifiedPath(context.getConf(), new Path(database.getLocationUri())));
    } else {
      // Location is not set we utilize METASTOREWAREHOUSE together with database name
      Path path = new Path(MetastoreConf.getVar(context.getConf(), MetastoreConf.ConfVars.WAREHOUSE),
          database.getName().toLowerCase() + DATABASE_PATH_SUFFIX);
      String qualifiedPath = Utilities.getQualifiedPath(context.getConf(), path);
      database.setLocationUri(qualifiedPath);
    }
  }
}
