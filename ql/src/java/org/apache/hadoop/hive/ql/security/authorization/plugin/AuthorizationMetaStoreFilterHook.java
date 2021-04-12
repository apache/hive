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
package org.apache.hadoop.hive.ql.security.authorization.plugin;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.common.classification.InterfaceAudience.Private;
import org.apache.hadoop.hive.metastore.DefaultMetaStoreFilterHookImpl;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * Metastore filter hook for filtering out the list of objects that the current authorization
 * implementation does not allow user to see
 */
@Private
public class AuthorizationMetaStoreFilterHook extends DefaultMetaStoreFilterHookImpl {

  public static final Logger LOG = LoggerFactory.getLogger(AuthorizationMetaStoreFilterHook.class);

  public AuthorizationMetaStoreFilterHook(Configuration conf) {
    super(conf);
  }

  @Override
  public List<String> filterTableNames(String catName, String dbName, List<String> tableList)
      throws MetaException {
    List<HivePrivilegeObject> listObjs = getHivePrivObjects(dbName, tableList);
    return getTableNames(getFilteredObjects(listObjs));
  }
  @Override
  public List<Table> filterTables(List<Table> tableList) throws MetaException {
    List<HivePrivilegeObject> listObjs = getHivePrivObjects(tableList);
    return getFilteredTableList(getFilteredObjects(listObjs),tableList);
  }

  private List<Table> getFilteredTableList(List<HivePrivilegeObject> hivePrivilegeObjects, List<Table> tableList) {
    List<Table> ret = new ArrayList<>();
    for(HivePrivilegeObject hivePrivilegeObject:hivePrivilegeObjects) {
      String dbName  = hivePrivilegeObject.getDbname();
      String tblName = hivePrivilegeObject.getObjectName();
      Table  table   = getFilteredTable(dbName,tblName,tableList);
      if (table != null) {
        ret.add(table);
      }
    }
    return ret;
  }

  private Table getFilteredTable(String dbName, String tblName, List<Table> tableList) {
    Table ret = null;
    for (Table table: tableList) {
      String databaseName = table.getDbName();
      String tableName = table.getTableName();
      if (dbName.equals(databaseName) && tblName.equals(tableName)) {
        ret = table;
        break;
      }
    }
    return ret;
  }

  @Override
  public List<String> filterDatabases(List<String> dbList) throws MetaException {
    List<HivePrivilegeObject> listObjs = HivePrivilegeObjectUtils.getHivePrivDbObjects(dbList);
    return getDbNames(getFilteredObjects(listObjs));
  }


  private List<String> getDbNames(List<HivePrivilegeObject> filteredObjects) {
    List<String> tnames = new ArrayList<String>();
    for(HivePrivilegeObject obj : filteredObjects) {
      tnames.add(obj.getDbname());
    }
    return tnames;
  }

  private List<String> getTableNames(List<HivePrivilegeObject> filteredObjects) {
    List<String> tnames = new ArrayList<String>();
    for(HivePrivilegeObject obj : filteredObjects) {
      tnames.add(obj.getObjectName());
    }
    return tnames;
  }

  private List<HivePrivilegeObject> getFilteredObjects(List<HivePrivilegeObject> listObjs) throws MetaException {
    SessionState ss = SessionState.get();
    HiveAuthzContext.Builder authzContextBuilder = new HiveAuthzContext.Builder();
    authzContextBuilder.setUserIpAddress(ss.getUserIpAddress());
    authzContextBuilder.setForwardedAddresses(ss.getForwardedAddresses());
    try {
      return ss.getAuthorizerV2().filterListCmdObjects(listObjs, authzContextBuilder.build());
    } catch (HiveAuthzPluginException e) {
      LOG.error("Authorization error", e);
      throw new MetaException(e.getMessage());
    } catch (HiveAccessControlException e) {
      // authorization error is not really expected in a filter call
      // the impl should have just filtered out everything. A checkPrivileges call
      // would have already been made to authorize this action
      LOG.error("AccessControlException", e);
      throw new MetaException(e.getMessage());
    }
  }

  private List<HivePrivilegeObject> getHivePrivObjects(String dbName, List<String> tableList) {
    List<HivePrivilegeObject> objs = new ArrayList<HivePrivilegeObject>();
    for(String tname : tableList) {
      objs.add(new HivePrivilegeObject(HivePrivilegeObjectType.TABLE_OR_VIEW, dbName, tname));
    }
    return objs;
  }

  private List<HivePrivilegeObject> getHivePrivObjects(List<Table> tableList) {
    List<HivePrivilegeObject> objs = new ArrayList<HivePrivilegeObject>();
    for(Table tableObject : tableList) {
      objs.add(new HivePrivilegeObject(HivePrivilegeObjectType.TABLE_OR_VIEW, tableObject.getDbName(), tableObject.getTableName(), null, null,
              HivePrivilegeObject.HivePrivObjectActionType.OTHER, null, null, tableObject.getOwner(), tableObject.getOwnerType()));
    }
    return objs;
  }

   @Override
   public List<TableMeta> filterTableMetas(String catName,String dbName,List<TableMeta> tableMetas) throws MetaException {
     List<String> tableNames = new ArrayList<>();
     for(TableMeta tableMeta: tableMetas){
       tableNames.add(tableMeta.getTableName());
     }
     List<String> filteredTableNames = filterTableNames(catName,dbName,tableNames);
     return tableMetas.stream()
             .filter(e -> filteredTableNames.contains(e.getTableName())).collect(Collectors.toList());
   }


}

