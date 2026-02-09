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
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DatabaseType;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.StringUtils;
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
    Database database = new Database(desc.getName(), desc.getComment(), desc.getLocationUri(),
        desc.getDatabaseProperties());
    database.setCatalogName(desc.getCatalogName());
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
      String defaultCatalog = MetastoreConf.get(context.getConf(), MetastoreConf.ConfVars.CATALOG_DEFAULT.getVarname());
      if (!StringUtils.isEmpty(defaultCatalog) && !defaultCatalog.equals(Warehouse.DEFAULT_CATALOG_NAME)) {
        database.setCatalogName(defaultCatalog);
      }
      context.getDb().createDatabase(database, desc.getIfNotExists());
    } catch (AlreadyExistsException ex) {
      //it would be better if AlreadyExistsException had an errorCode field....
      throw new HiveException(ex, ErrorMsg.DATABASE_ALREADY_EXISTS, desc.getName());
    }

    return 0;
  }

  private void makeLocationQualified(Database database) throws HiveException {
    String catalogName = database.getCatalogName().toLowerCase();
    String dbName = database.getName().toLowerCase();
    boolean isDefaultCatalog = Warehouse.DEFAULT_CATALOG_NAME.equalsIgnoreCase(catalogName);

    // -------- External location --------
    if (database.isSetLocationUri()) {
      database.setLocationUri(Utilities.getQualifiedPath(context.getConf(), new Path(database.getLocationUri())));
    } else {
      String rootDir = getExternalRootDir(isDefaultCatalog);
      Path path = buildDbPath(rootDir, catalogName, dbName, isDefaultCatalog);
      database.setLocationUri(Utilities.getQualifiedPath(context.getConf(), path));
    }

    // -------- Managed location --------
    if (database.isSetManagedLocationUri()) {
      database.setManagedLocationUri(Utilities.getQualifiedPath(context.getConf(),
          new Path(database.getManagedLocationUri())));
    } else {
      String rootDir = MetastoreConf.getVar(
              context.getConf(),
              isDefaultCatalog
                      ? MetastoreConf.ConfVars.WAREHOUSE
                      : MetastoreConf.ConfVars.WAREHOUSE_CATALOG
      );

      Path path = buildDbPath(rootDir, catalogName, dbName, isDefaultCatalog);
      String qualifiedPath = Utilities.getQualifiedPath(context.getConf(), path);
      if (!qualifiedPath.equals(database.getLocationUri())) {
        database.setManagedLocationUri(qualifiedPath);
      }
    }
  }

  private Path buildDbPath(String rootDir, String catalogName, String dbName, boolean isDefaultCatalog) {
    return isDefaultCatalog
            ? new Path(rootDir, dbName + DATABASE_PATH_SUFFIX)
            : new Path(rootDir + "/" + catalogName, dbName + DATABASE_PATH_SUFFIX);
  }

  private String getExternalRootDir(boolean isDefaultCatalog) {
    MetastoreConf.ConfVars externalVar = isDefaultCatalog
            ? MetastoreConf.ConfVars.WAREHOUSE_EXTERNAL
            : MetastoreConf.ConfVars.WAREHOUSE_CATALOG_EXTERNAL;

    String rootDir = MetastoreConf.getVar(context.getConf(), externalVar);
    if (rootDir != null && !rootDir.trim().isEmpty()) {
      return rootDir;
    }

    MetastoreConf.ConfVars fallbackVar = isDefaultCatalog
            ? MetastoreConf.ConfVars.WAREHOUSE
            : MetastoreConf.ConfVars.WAREHOUSE_CATALOG;

    LOG.warn("{} is not set, falling back to {}. This could cause external tables to use managed tablespace.",
            externalVar.getVarname(), fallbackVar.getVarname());

    return MetastoreConf.getVar(context.getConf(), fallbackVar);
  }
}
