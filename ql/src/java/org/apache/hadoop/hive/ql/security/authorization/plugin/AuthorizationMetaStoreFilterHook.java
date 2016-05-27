/**
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.common.classification.InterfaceAudience.Private;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.DefaultMetaStoreFilterHookImpl;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * Metastore filter hook for filtering out the list of objects that the current authorization
 * implementation does not allow user to see
 */
@Private
public class AuthorizationMetaStoreFilterHook extends DefaultMetaStoreFilterHookImpl {

  public static final Logger LOG = LoggerFactory.getLogger(AuthorizationMetaStoreFilterHook.class);

  public AuthorizationMetaStoreFilterHook(HiveConf conf) {
    super(conf);
  }

  @Override
  public List<String> filterTableNames(String dbName, List<String> tableList) throws MetaException {
    List<HivePrivilegeObject> listObjs = getHivePrivObjects(dbName, tableList);
    return getTableNames(getFilteredObjects(listObjs));
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

}

