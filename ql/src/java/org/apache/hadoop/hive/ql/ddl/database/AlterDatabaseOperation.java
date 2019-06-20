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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Operation process of altering a database.
 */
public class AlterDatabaseOperation extends DDLOperation<AlterDatabaseDesc> {
  public AlterDatabaseOperation(DDLOperationContext context, AlterDatabaseDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    String dbName = desc.getDatabaseName();
    Database database = context.getDb().getDatabase(dbName);
    if (database == null) {
      throw new HiveException(ErrorMsg.DATABASE_NOT_EXISTS, dbName);
    }

    Map<String, String> params = database.getParameters();
    if ((null != desc.getReplicationSpec()) &&
        !desc.getReplicationSpec().allowEventReplacementInto(params)) {
      LOG.debug("DDLTask: Alter Database {} is skipped as database is newer than update", dbName);
      return 0; // no replacement, the existing database state is newer than our update.
    }

    switch (desc.getAlterType()) {
    case ALTER_PROPERTY:
      alterProperties(database, params);
      break;

    case ALTER_OWNER:
      alterOwner(database);
      break;

    case ALTER_LOCATION:
      alterLocation(database);
      break;

    default:
      throw new AssertionError("Unsupported alter database type! : " + desc.getAlterType());
    }

    context.getDb().alterDatabase(database.getName(), database);
    return 0;
  }

  private void alterProperties(Database database, Map<String, String> params) {
    Map<String, String> newParams = desc.getDatabaseProperties();

    // if both old and new params are not null, merge them
    if (params != null && newParams != null) {
      params.putAll(newParams);
      database.setParameters(params);
    } else {
      // if one of them is null, replace the old params with the new one
      database.setParameters(newParams);
    }
  }

  private void alterOwner(Database database) {
    database.setOwnerName(desc.getOwnerPrincipal().getName());
    database.setOwnerType(desc.getOwnerPrincipal().getType());
  }

  private void alterLocation(Database database) throws HiveException {
    try {
      String newLocation = desc.getLocation();
      URI locationURI = new URI(newLocation);
      if (!locationURI.isAbsolute() || StringUtils.isBlank(locationURI.getScheme())) {
        throw new HiveException(ErrorMsg.BAD_LOCATION_VALUE, newLocation);
      }

      if (newLocation.equals(database.getLocationUri())) {
        LOG.info("AlterDatabase skipped. No change in location.");
      } else {
        database.setLocationUri(newLocation);
      }
    } catch (URISyntaxException e) {
      throw new HiveException(e);
    }
  }
}
