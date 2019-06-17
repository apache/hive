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

package org.apache.hadoop.hive.ql.security.authorization.command;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionUtils;
import org.apache.hadoop.hive.ql.exec.FunctionInfo.FunctionType;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.hooks.Entity.Type;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.security.authorization.AuthorizationUtils;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivObjectActionType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * Command authorization, new type.
 */
final class CommandAuthorizerV2 {
  private CommandAuthorizerV2() {
    throw new UnsupportedOperationException("CommandAuthorizerV2 should not be instantiated");
  }

  static void doAuthorization(HiveOperation op, BaseSemanticAnalyzer sem, SessionState ss,
      Set<ReadEntity> inputs, Set<WriteEntity> outputs, String command) throws HiveException {
    HiveOperationType hiveOpType = HiveOperationType.valueOf(op.name());

    // colAccessInfo is set only in case of SemanticAnalyzer
    Map<String, List<String>> selectTab2Cols = sem.getColumnAccessInfo() != null
        ? sem.getColumnAccessInfo().getTableToColumnAccessMap() : null;
    Map<String, List<String>> updateTab2Cols = sem.getUpdateColumnAccessInfo() != null
        ? sem.getUpdateColumnAccessInfo().getTableToColumnAccessMap() : null;

    List<ReadEntity> inputList = new ArrayList<ReadEntity>(inputs);
    List<WriteEntity> outputList = new ArrayList<WriteEntity>(outputs);
    addPermanentFunctionEntities(ss, inputList);

    List<HivePrivilegeObject> inputsHObjs = getHivePrivObjects(inputList, selectTab2Cols);
    List<HivePrivilegeObject> outputHObjs = getHivePrivObjects(outputList, updateTab2Cols);

    HiveAuthzContext.Builder authzContextBuilder = new HiveAuthzContext.Builder();
    authzContextBuilder.setUserIpAddress(ss.getUserIpAddress());
    authzContextBuilder.setForwardedAddresses(ss.getForwardedAddresses());
    authzContextBuilder.setCommandString(command);

    ss.getAuthorizerV2().checkPrivileges(hiveOpType, inputsHObjs, outputHObjs, authzContextBuilder.build());
  }

  private static void addPermanentFunctionEntities(SessionState ss, List<ReadEntity> inputList) throws HiveException {
    for (Entry<String, FunctionInfo> function : ss.getCurrentFunctionsInUse().entrySet()) {
      if (function.getValue().getFunctionType() != FunctionType.PERSISTENT) {
        // Built-in function access is allowed to all users. If user can create a temp function, they may use it.
        continue;
      }

      String[] qualifiedFunctionName = FunctionUtils.getQualifiedFunctionNameParts(function.getKey());
      // this is only for the purpose of authorization, only the name matters.
      Database db = new Database(qualifiedFunctionName[0], "", "", null);
      inputList.add(new ReadEntity(db, qualifiedFunctionName[1], function.getValue().getClassName(), Type.FUNCTION));
    }
  }

  private static List<HivePrivilegeObject> getHivePrivObjects(List<? extends Entity> privObjects,
      Map<String, List<String>> tableName2Cols) {
    List<HivePrivilegeObject> hivePrivobjs = new ArrayList<HivePrivilegeObject>();
    if (privObjects == null){
      return hivePrivobjs;
    }

    for (Entity privObject : privObjects){
      if (privObject.isDummy()) {
        //do not authorize dummy readEntity or writeEntity
        continue;
      }
      if (privObject instanceof ReadEntity && !((ReadEntity)privObject).isDirect()) {
        // This ReadEntity represents one of the underlying tables/views of a view, so skip it.
        continue;
      }
      if (privObject instanceof WriteEntity && ((WriteEntity)privObject).isTempURI()) {
        // do not authorize temporary uris
        continue;
      }
      if (privObject.getTyp() == Type.TABLE && (privObject.getT() == null || privObject.getT().isTemporary())) {
        // skip temporary tables from authorization
        continue;
      }

      addHivePrivObject(privObject, tableName2Cols, hivePrivobjs);
    }
    return hivePrivobjs;
  }

  private static void addHivePrivObject(Entity privObject, Map<String, List<String>> tableName2Cols,
      List<HivePrivilegeObject> hivePrivObjs) {
    HivePrivilegeObjectType privObjType = AuthorizationUtils.getHivePrivilegeObjectType(privObject.getType());
    HivePrivObjectActionType actionType = AuthorizationUtils.getActionType(privObject);
    HivePrivilegeObject hivePrivObject = null;
    switch(privObject.getType()){
    case DATABASE:
      Database database = privObject.getDatabase();
      hivePrivObject = new HivePrivilegeObject(privObjType, database.getName(), null, null, null, actionType, null,
          null, database.getOwnerName(), database.getOwnerType());
      break;
    case TABLE:
      Table table = privObject.getTable();
      List<String> columns = tableName2Cols == null ? null :
          tableName2Cols.get(Table.getCompleteName(table.getDbName(), table.getTableName()));
      hivePrivObject = new HivePrivilegeObject(privObjType, table.getDbName(), table.getTableName(),
          null, columns, actionType, null, null, table.getOwner(), table.getOwnerType());
      break;
    case DFS_DIR:
    case LOCAL_DIR:
      hivePrivObject = new HivePrivilegeObject(privObjType, null, privObject.getD().toString(), null, null,
          actionType, null, null, null, null);
      break;
    case FUNCTION:
      String dbName = privObject.getDatabase() != null ? privObject.getDatabase().getName() : null;
      hivePrivObject = new HivePrivilegeObject(privObjType, dbName, privObject.getFunctionName(),
          null, null, actionType, null, privObject.getClassName(), null, null);
      break;
    case DUMMYPARTITION:
    case PARTITION:
      // TODO: not currently handled
      return;
    case SERVICE_NAME:
      hivePrivObject = new HivePrivilegeObject(privObjType, null, privObject.getServiceName(), null,
          null, actionType, null, null, null, null);
      break;
    default:
      throw new AssertionError("Unexpected object type");
    }
    hivePrivObjs.add(hivePrivObject);
  }
}
