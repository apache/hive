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

package org.apache.hadoop.hive.ql.ddl.database.create;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DatabaseType;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.database.desc.DescDatabaseDesc;
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
    Database database = new Database(desc.getName(), desc.getComment(), desc.getLocationUri(),
        desc.getDatabaseProperties());
    database.setOwnerName(SessionState.getUserFromAuthenticator());
    database.setOwnerType(PrincipalType.USER);
    database.setType(desc.getDatabaseType());
    try {
      if (desc.getDatabaseType() == DatabaseType.NATIVE) {
        if (desc.getManagedLocationUri() != null) {
          database.setManagedLocationUri(desc.getManagedLocationUri());
        }
        makeLocationQualified(database);
        if (database.getLocationUri().equalsIgnoreCase(database.getManagedLocationUri())) {
          throw new HiveException("Managed and external locations for database cannot be the same");
        }
      } else if (desc.getDatabaseType() == DatabaseType.REMOTE) {
        makeLocationQualified(database);
        database.setConnector_name(desc.getConnectorName());
        database.setRemote_dbname(desc.getRemoteDbName());
      } else { // should never be here
        throw new HiveException("Unsupported database type " + database.getType() + " for " + database.getName());
      }
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
      // Location is not set we utilize WAREHOUSE_EXTERNAL together with database name
      String rootDir = MetastoreConf.getVar(context.getConf(), MetastoreConf.ConfVars.WAREHOUSE_EXTERNAL);
      if (rootDir == null || rootDir.trim().isEmpty()) {
        // Fallback plan
        LOG.warn(String.format(
            "%s is not set, falling back to %s. This could cause external tables to use to managed tablespace.",
            MetastoreConf.ConfVars.WAREHOUSE_EXTERNAL.getVarname(), MetastoreConf.ConfVars.WAREHOUSE.getVarname()));
        rootDir = MetastoreConf.getVar(context.getConf(), MetastoreConf.ConfVars.WAREHOUSE);
      }
      Path path = new Path(rootDir, database.getName().toLowerCase() + DATABASE_PATH_SUFFIX);
      String qualifiedPath = Utilities.getQualifiedPath(context.getConf(), path);
      database.setLocationUri(qualifiedPath);
    }

    if (database.isSetManagedLocationUri()) {
      database.setManagedLocationUri(Utilities.getQualifiedPath(context.getConf(),
          new Path(database.getManagedLocationUri())));
    } else {
      // ManagedLocation is not set we utilize WAREHOUSE together with database name 
      String rootDir = MetastoreConf.getVar(context.getConf(), MetastoreConf.ConfVars.WAREHOUSE);
      Path path = new Path(rootDir, database.getName().toLowerCase() + DATABASE_PATH_SUFFIX);
      String qualifiedPath = Utilities.getQualifiedPath(context.getConf(), path);
      if (!qualifiedPath.equals(database.getLocationUri())) {
        database.setManagedLocationUri(qualifiedPath);
      }
    }
  }
}
