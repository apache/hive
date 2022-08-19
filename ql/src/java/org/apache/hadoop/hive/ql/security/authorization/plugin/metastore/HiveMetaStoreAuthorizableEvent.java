/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.security.authorization.plugin.metastore;

import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;

import java.util.Collections;
import java.util.List;

/*
HiveMetaStoreAuthorizableEvent: Abstract class for getting the MetaStore Event context for HiveMetaStore Authorization
 */

public abstract class HiveMetaStoreAuthorizableEvent {
  protected final PreEventContext preEventContext;

  protected HiveMetaStoreAuthorizableEvent(PreEventContext preEventContext) {
    this.preEventContext = preEventContext;
  }

  public abstract HiveMetaStoreAuthzInfo getAuthzContext();

  protected String getSdLocation(StorageDescriptor sd) {
    return sd == null ? "" : sd.getLocation();
  }

  protected List<String> getCommandParams(String cmdStr, String objectName) {
    String commandString = (objectName != null) ? cmdStr + " " + objectName : cmdStr;

    return Collections.singletonList(commandString);
  }

  protected HivePrivilegeObject getHivePrivilegeObject(Database database) {
    return new HivePrivilegeObject(HivePrivilegeObject.HivePrivilegeObjectType.DATABASE, database.getName(),
        null, null, null, HivePrivilegeObject.HivePrivObjectActionType.OTHER, null, null,
        database.getOwnerName(), database.getOwnerType());
  }

  protected HivePrivilegeObject getHivePrivilegeObject(Table table) {
    return new HivePrivilegeObject(HivePrivilegeObject.HivePrivilegeObjectType.TABLE_OR_VIEW, table.getDbName(),
        table.getTableName(), null, null, HivePrivilegeObject.HivePrivObjectActionType.OTHER, null, null,
        table.getOwner(), table.getOwnerType());
  }

  protected HivePrivilegeObject getHivePrivilegeObjectDfsUri(String uri) {
    return new HivePrivilegeObject(HivePrivilegeObject.HivePrivilegeObjectType.DFS_URI, null, uri);
  }

  protected HivePrivilegeObject getHivePrivilegeObjectLocalUri(String uri) {
    return new HivePrivilegeObject(HivePrivilegeObject.HivePrivilegeObjectType.LOCAL_URI, null, uri);
  }

  protected HivePrivilegeObject getHivePrivilegeObject(DataConnector connector) {
    return new HivePrivilegeObject(HivePrivilegeObject.HivePrivilegeObjectType.DATACONNECTOR, null,
        connector.getName(), null, null, HivePrivilegeObject.HivePrivObjectActionType.OTHER, null, null,
        connector.getOwnerName(), connector.getOwnerType());
  }

}
