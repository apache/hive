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
package org.apache.hadoop.hive.ql.security.authorization;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.PrincipalDesc;
import org.apache.hadoop.hive.ql.plan.PrivilegeDesc;
import org.apache.hadoop.hive.ql.plan.PrivilegeObjectDesc;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizationTranslator;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilege;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;


/**
 * Default implementation of HiveAuthorizationTranslator
 */
public class DefaultHiveAuthorizationTranslator implements HiveAuthorizationTranslator {

  @Override
  public HivePrincipal getHivePrincipal(PrincipalDesc principal) throws HiveException {
    if (principal == null) {
      return null;
    }
    return AuthorizationUtils.getHivePrincipal(principal.getName(), principal.getType());
  }

  @Override
  public HivePrivilege getHivePrivilege(PrivilegeDesc privilege) {
    Privilege priv = privilege.getPrivilege();
    return new HivePrivilege(priv.toString(), privilege.getColumns(), priv.getScopeList());
  }

  @Override
  public HivePrivilegeObject getHivePrivilegeObject(PrivilegeObjectDesc privSubjectDesc)
      throws HiveException {
    // null means ALL for show grants, GLOBAL for grant/revoke
    HivePrivilegeObjectType objectType = null;

    String[] dbTable;
    List<String> partSpec = null;
    List<String> columns = null;
    if (privSubjectDesc == null) {
      dbTable = new String[] {null, null};
    } else {
      if (privSubjectDesc.getTable()) {
        dbTable = Utilities.getDbTableName(privSubjectDesc.getObject());
      } else {
        dbTable = new String[] {privSubjectDesc.getObject(), null};
      }
      if (privSubjectDesc.getPartSpec() != null) {
        partSpec = new ArrayList<String>(privSubjectDesc.getPartSpec().values());
      }
      columns = privSubjectDesc.getColumns();
      objectType = AuthorizationUtils.getPrivObjectType(privSubjectDesc);
    }
    return new HivePrivilegeObject(objectType, dbTable[0], dbTable[1], partSpec, columns, null);
  }

  @Override
  public List<HivePrivilegeObject> getHivePrivObjectsFromEntity(
      Set<? extends Entity> privObjects,
      Map<String, List<String>> tableName2Cols) {
    List<HivePrivilegeObject> hivePrivobjs = new ArrayList<HivePrivilegeObject>();
    if(privObjects == null){
      return hivePrivobjs;
    }
    for(Entity privObject : privObjects){
      HivePrivilegeObjectType privObjType =
          AuthorizationUtils.getHivePrivilegeObjectType(privObject.getType());
      if(privObject.isDummy()) {
        //do not authorize dummy readEntity or writeEntity
        continue;
      }
      if(privObject instanceof ReadEntity && !((ReadEntity)privObject).isDirect()){
        // In case of views, the underlying views or tables are not direct dependencies
        // and are not used for authorization checks.
        // This ReadEntity represents one of the underlying tables/views, so skip it.
        // See description of the isDirect in ReadEntity
        continue;
      }
      if(privObject instanceof WriteEntity && ((WriteEntity)privObject).isTempURI()){
        //do not authorize temporary uris
        continue;
      }
      //support for authorization on partitions needs to be added
      String dbname = null;
      String objName = null;
      List<String> partKeys = null;
      List<String> columns = null;
      switch(privObject.getType()){
        case DATABASE:
          dbname = privObject.getDatabase().getName();
          break;
        case TABLE:
          dbname = privObject.getTable().getDbName();
          objName = privObject.getTable().getTableName();
          columns = tableName2Cols == null ? null :
              tableName2Cols.get(Table.getCompleteName(dbname, objName));
          break;
        case DFS_DIR:
        case LOCAL_DIR:
          objName = privObject.getD().toString();
          break;
        case FUNCTION:
          if(privObject.getDatabase() != null) {
            dbname = privObject.getDatabase().getName();
          }
          objName = privObject.getFunctionName();
          break;
        case DUMMYPARTITION:
        case PARTITION:
          // not currently handled
          continue;
        default:
          throw new AssertionError("Unexpected object type");
      }
      HivePrivilegeObject.HivePrivObjectActionType actionType = AuthorizationUtils.getActionType(privObject);
      HivePrivilegeObject hPrivObject = new HivePrivilegeObject(privObjType, dbname, objName,
          partKeys, columns, actionType, null);
      hivePrivobjs.add(hPrivObject);
    }
    return hivePrivobjs;
  }
}
