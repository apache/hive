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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.common.classification.InterfaceAudience.Private;
import org.apache.hadoop.hive.metastore.DefaultMetaStoreFilterHookImpl;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import static org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObjectUtils.TablePrivilegeLookup;
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
    List<HivePrivilegeObject> listObjs = getHivePrivObjects(catName, dbName, tableList);
    return getFilteredObjectNames(getFilteredObjects(listObjs));
  }

  /**
   * Filters the given list of tables down to those the current user is authorized to see.
   *
   * <p>The method delegates authorization decisions to {@link HiveAuthorizer#filterListCmdObjects},
   * then uses a {@link TablePrivilegeLookup} (binary-search index) to match the returned
   * privilege objects back to the original {@link Table} objects in O(n log n), replacing an
   * earlier O(nÂ²) nested-loop implementation that was prohibitively slow for large table lists.
   *
   * <p>A table's catName is normalized to the default catalog before the lookup when it is
   * {@code null}, consistent with how {@code tablesToPrivilegeObjs} builds the privilege objects
   * that are sent to the authorizer.
   *
   * @param tableList the full list of tables returned by the MetaStore before authorization
   * @return the sub-list of tables the current user is permitted to list
   * @throws MetaException if the authorizer throws an exception
   */
  @Override
  public List<Table> filterTables(List<Table> tableList) throws MetaException {
    List<HivePrivilegeObject> listObjs = tablesToPrivilegeObjs(tableList);
    TablePrivilegeLookup index = new TablePrivilegeLookup(getFilteredObjects(listObjs));
    List<Table> ret = new ArrayList<>();
    String defaultCatName = MetaStoreUtils.getDefaultCatalog(HivePrivilegeObject.getConf());
    for (Table table : tableList) {
      String catName = table.getCatName() != null ? table.getCatName() : defaultCatName;
      if (index.lookup(catName, table.getDbName(), table.getTableName()) != null) {
        ret.add(table);
      }
    }
    return ret;
  }

  @Override
  public List<String> filterDatabases(String catName, List<String> dbList) throws MetaException {
    List<HivePrivilegeObject> listObjs = HivePrivilegeObjectUtils.getHivePrivDbObjects(catName, dbList);
    return getDbNames(getFilteredObjects(listObjs));
  }


  private List<String> getDbNames(List<HivePrivilegeObject> filteredObjects) {
    List<String> tnames = new ArrayList<String>();
    for(HivePrivilegeObject obj : filteredObjects) {
      tnames.add(obj.getDbname());
    }
    return tnames;
  }

  private List<String> getFilteredObjectNames(List<HivePrivilegeObject> filteredObjects) {
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

  private List<HivePrivilegeObject> getHivePrivObjects(String catName, String dbName, List<String> tableList) {
    List<HivePrivilegeObject> objs = new ArrayList<HivePrivilegeObject>();
    for(String tname : tableList) {
      objs.add(new HivePrivilegeObject(HivePrivilegeObjectType.TABLE_OR_VIEW, catName, dbName, tname));
    }
    return objs;
  }

  private HivePrivilegeObject createPrivilegeObjectForTable(String catName, String dbName, String tableName,
      String owner, PrincipalType ownerType) {
    return new HivePrivilegeObject(HivePrivilegeObjectType.TABLE_OR_VIEW, catName, dbName, tableName, null, null,
        HivePrivilegeObject.HivePrivObjectActionType.OTHER, null, null, owner, ownerType);
  }

  private List<HivePrivilegeObject> tablesToPrivilegeObjs(List<Table> tableList) {
    List<HivePrivilegeObject> objs = new ArrayList<HivePrivilegeObject>();
    for (Table tableObject : tableList) {
      objs.add(createPrivilegeObjectForTable(tableObject.getCatName(), tableObject.getDbName(), tableObject.getTableName(),
          tableObject.getOwner(), tableObject.getOwnerType()));
    }
    return objs;
  }

  private List<HivePrivilegeObject> tableMetasToPrivilegeObjs(List<TableMeta> tableMetas) {
    List<HivePrivilegeObject> objs = new ArrayList<HivePrivilegeObject>();
    for (TableMeta tableMeta : tableMetas) {
      objs.add(createPrivilegeObjectForTable(tableMeta.getCatName(), tableMeta.getDbName(), tableMeta.getTableName(),
          tableMeta.getOwnerName(), tableMeta.getOwnerType()));
    }
    return objs;
  }

  @Override
  public List<TableMeta> filterTableMetas(List<TableMeta> tableMetas) throws MetaException {
    List<HivePrivilegeObject> listObjs = tableMetasToPrivilegeObjs(tableMetas);
    List<HivePrivilegeObject> filteredList = getFilteredObjects(listObjs);
    final List<TableMeta> ret = new ArrayList<>();
    final TablePrivilegeLookup index = new TablePrivilegeLookup(filteredList);
    for(TableMeta table : tableMetas) {
      if (index.lookup(table.getCatName(), table.getDbName(), table.getTableName()) != null) {
        ret.add(table);
      }
    }
    return ret;
  }

  @Override
  public List<String> filterDataConnectors(List<String> dcList) throws MetaException {
    List<HivePrivilegeObject> listObjs = HivePrivilegeObjectUtils.getHivePrivDcObjects(dcList);
    return getFilteredObjectNames(getFilteredObjects(listObjs));
  }
}

