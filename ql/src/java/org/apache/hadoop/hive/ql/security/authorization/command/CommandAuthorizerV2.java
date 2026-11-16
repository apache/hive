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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.security.authorization.command;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command authorization, new type.
 */
final class CommandAuthorizerV2 {
  private static final Logger LOG = LoggerFactory.getLogger(CommandAuthorizerV2.class.getName());
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
    enrichAvroSchemaUrlInputs(inputList);

    List<HivePrivilegeObject> inputsHObjs = getHivePrivObjects(inputList, selectTab2Cols, hiveOpType, sem);
    List<HivePrivilegeObject> outputHObjs = getHivePrivObjects(outputList, updateTab2Cols, hiveOpType, sem);

    HiveAuthzContext.Builder authzContextBuilder = new HiveAuthzContext.Builder();
    authzContextBuilder.setUserIpAddress(ss.getUserIpAddress());
    authzContextBuilder.setForwardedAddresses(ss.getForwardedAddresses());
    authzContextBuilder.setCommandString(command);

    ss.getAuthorizerV2().checkPrivileges(hiveOpType, inputsHObjs, outputHObjs, authzContextBuilder.build());
  }

  private static void enrichAvroSchemaUrlInputs(List<ReadEntity> inputList) {
    List<ReadEntity> snapshot = new ArrayList<>(inputList);
    for (ReadEntity readEntity : snapshot) {
      AuthorizationUtils.addAvroSchemaUrlInputForReadEntity(inputList, readEntity);
    }
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
      Map<String, List<String>> tableName2Cols, HiveOperationType hiveOpType, BaseSemanticAnalyzer sem) throws HiveException {
    List<HivePrivilegeObject> hivePrivobjs = new ArrayList<HivePrivilegeObject>();
    if (privObjects == null) {
      return hivePrivobjs;
    }

    // Pre-scan once to collect base tables that are accessed via a regular view.
    Set<String> baseTablesViaRegularView = buildBaseTablesViaRegularView(privObjects);

    for (Entity privObject : privObjects) {
      if (privObject.isDummy()) {
        //do not authorize dummy readEntity or writeEntity
        continue;
      }
      if (privObject instanceof ReadEntity && !((ReadEntity)privObject).isDirect()) {
        // This ReadEntity represents one of the underlying tables/views of a view, skip it if
        // it's not inside a deferred authorized view.
        ReadEntity reTable = (ReadEntity)privObject;
        Boolean isDeferred = false;
        if ( reTable.getParents() != null && reTable.getParents().size() > 0) {
          for ( ReadEntity re: reTable.getParents()){
            if (re.getTyp() == Type.TABLE && re.getTable() != null ) {
              Table t = re.getTable();
              if (!isDeferredAuthView(t)) {
                continue;
              } else {
                isDeferred = true;
              }
            }
          }
        }
        if (!isDeferred) {
          continue;
        }
      }
      if (privObject instanceof WriteEntity && ((WriteEntity)privObject).isTempURI()) {
        // do not authorize temporary uris
        continue;
      }
      if (privObject.getTyp() == Type.TABLE && (privObject.getT() == null || privObject.getT().isTemporary())) {
        // skip temporary tables from authorization
        continue;
      }

      if (privObject.getTyp() == Type.FUNCTION && !HiveConf.getBoolVar(SessionState.get().getConf(),
              HiveConf.ConfVars.HIVE_AUTHORIZATION_FUNCTIONS_IN_VIEW) && hiveOpType == HiveOperationType.QUERY) {
        String[] qualifiedFunctionName = new String[]{privObject.getDatabase() != null ?
                privObject.getDatabase().getName() :  null, privObject.getFunctionName()};
        if (!sem.getUserSuppliedFunctions().contains(qualifiedFunctionName[0] + "." + qualifiedFunctionName[1])) {
          continue;
        }
      }
      if (isPartitionAccessedViaRegularView(privObject, baseTablesViaRegularView)) {
        // skip Partition Entity auth for regular view
        continue;
      }

      addHivePrivObject(privObject, tableName2Cols, hivePrivobjs, hiveOpType);
    }
    return hivePrivobjs;
  }

  /**
   * Pre-scans the entity list once and returns the set of "db.table" keys for base tables
   * that are accessed indirectly via a regular view.
   */
  private static Set<String> buildBaseTablesViaRegularView(List<? extends Entity> entities) {
    Set<String> result = new HashSet<>();
    for (Entity entity : entities) {
      if (!(entity instanceof ReadEntity) || entity.getTyp() != Type.TABLE) {
        continue;
      }
      ReadEntity re = (ReadEntity) entity;
      Table t = re.getTable();
      if (!re.isDirect() && t != null && !hasDeferredViewParent(re) && hasRegularViewParent(re)) {
        result.add(t.getDbName() + "." + t.getTableName());
      }
    }
    return result;
  }

  /**
   * A deferred authorization view is view created by non-super user like spark-user. This view contains a parameter "Authorized"
   * set to false, so ranger will not authorize it during view creation. When a select statement is issued, then the ranger authorizes
   * the under lying tables.
   * @param t the HMS table object
   * @return boolean value
   */
  private static boolean isDeferredAuthView(Table t){
    String authorizedKeyword = "Authorized";
    if (t.isView() || t.isMaterializedView()) {
      Map<String, String> params = t.getParameters();
      if (params != null && params.containsKey(authorizedKeyword)) {
        String authorizedValue = params.get(authorizedKeyword);
        if ("false".equalsIgnoreCase(authorizedValue)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Returns true when a PARTITION entity should not produce its own privilege object
   * because access is already covered by a view's TABLE_OR_VIEW object.
   */
  private static boolean isPartitionAccessedViaRegularView(Entity entity,
      Set<String> baseTablesViaRegularView) {
    if (!(entity instanceof ReadEntity)
        || (entity.getTyp() != Type.PARTITION && entity.getTyp() != Type.DUMMYPARTITION)) {
      return false;
    }
    ReadEntity re = (ReadEntity) entity;
    // Deferred-auth views must still authorize the underlying base table.
    if (hasDeferredViewParent(re)) {
      return false;
    }

    if (hasRegularViewParent(re)) {
      return true;
    }
    Table partTable = re.getTable();
    return partTable != null
        && baseTablesViaRegularView.contains(partTable.getDbName() + "." + partTable.getTableName());
  }

  private static boolean hasDeferredViewParent(ReadEntity entity) {
    Set<ReadEntity> parents = entity.getParents();
    if (parents == null || parents.isEmpty()) {
      return false;
    }
    for (ReadEntity parent : parents) {
      if (parent.getTyp() == Type.TABLE && parent.getTable() != null
          && isDeferredAuthView(parent.getTable())) {
        return true;
      }
    }
    return false;
  }

  private static boolean hasRegularViewParent(ReadEntity entity) {
    Set<ReadEntity> parents = entity.getParents();
    if (parents == null || parents.isEmpty()) {
      return false;
    }
    for (ReadEntity parent : parents) {
      if (parent.getTyp() == Type.TABLE && parent.getTable() != null
          && (parent.getTable().isView() || parent.getTable().isMaterializedView())
          && !isDeferredAuthView(parent.getTable())) {
        return true;
      }
    }
    return false;
  }

  private static String catNameOrCurrent(String catName) {
    return catName != null ? catName : SessionState.get().getCurrentCatalog();
  }

  private static void addHivePrivObject(Entity privObject, Map<String, List<String>> tableName2Cols,
      List<HivePrivilegeObject> hivePrivObjs, HiveOperationType hiveOpType) throws HiveException {
    HivePrivilegeObjectType privObjType = AuthorizationUtils.getHivePrivilegeObjectType(privObject.getType());
    HivePrivObjectActionType actionType = AuthorizationUtils.getActionType(privObject);
    HivePrivilegeObject hivePrivObject = null;
    switch(privObject.getType()){
    case DATABASE:
      Database database = privObject.getDatabase();
      hivePrivObject = HivePrivilegeObject.builder(privObjType)
          .catName(catNameOrCurrent(database.getCatalogName()))
          .dbName(database.getName())
          .actionType(actionType)
          .ownerName(database.getOwnerName())
          .ownerType(database.getOwnerType())
          .build();
      break;
    case TABLE:
      Table table = privObject.getTable();
      List<String> columns = tableName2Cols == null ? null :
          tableName2Cols.get(Table.getCompleteName(table.getDbName(), table.getTableName()));
      hivePrivObject = HivePrivilegeObject.builder(privObjType)
          .catName(catNameOrCurrent(table.getCatalogName()))
          .dbName(table.getDbName())
          .objectName(table.getTableName())
          .columns(columns)
          .actionType(actionType)
          .ownerName(table.getOwner())
          .ownerType(table.getOwnerType())
          .build();
      if (table.getStorageHandler() != null && HiveConf.getBoolVar(SessionState.getSessionConf(),
          HiveConf.ConfVars.HIVE_AUTHORIZATION_TABLES_ON_STORAGEHANDLERS)) {
        //TODO: add hive privilege object for storage based handlers for create and alter table commands.
        if (privObject instanceof WriteEntity &&
                (hiveOpType == HiveOperationType.CREATETABLE ||
                hiveOpType == HiveOperationType.ALTERTABLE_PROPERTIES ||
                hiveOpType == HiveOperationType.CREATETABLE_AS_SELECT)) {
          try {
            String storageUri = table.getStorageHandler().getURIForAuth(table.getTTable()).toString();
            hivePrivObjs.add(HivePrivilegeObject.builder(HivePrivilegeObjectType.STORAGEHANDLER_URI)
                .objectName(storageUri)
                .actionType(actionType)
                .className(table.getStorageHandler().getClass().getName())
                .ownerName(table.getOwner())
                .ownerType(table.getOwnerType())
                .build());
          } catch (Exception ex) {
            LOG.error("Exception occurred while getting the URI from storage handler: " + ex.getMessage(), ex);
            throw new HiveException("Exception occurred while getting the URI from storage handler: " + ex.getMessage());
          }
        }
      }
      break;
    case DFS_DIR:
    case LOCAL_DIR:
      hivePrivObject = HivePrivilegeObject.builder(privObjType)
          .objectName(privObject.getD().toString())
          .actionType(actionType)
          .build();
      break;
    case FUNCTION:
      if (privObject.getFunction() != null) {
        Function function = privObject.getFunction();
        hivePrivObject = HivePrivilegeObject.builder(privObjType)
            .catName(catNameOrCurrent(function.getCatName()))
            .dbName(function.getDbName())
            .objectName(function.getFunctionName())
            .actionType(actionType)
            .className(function.getClassName())
            .ownerName(function.getOwnerName())
            .ownerType(function.getOwnerType())
            .build();
      } else {
        String catName = null;
        String dbName = null;
        if (privObject.getDatabase() != null) {
          catName = catNameOrCurrent(privObject.getDatabase().getCatalogName());
          dbName = privObject.getDatabase().getName();
        }
        hivePrivObject = HivePrivilegeObject.builder(privObjType)
            .catName(catName)
            .dbName(dbName)
            .objectName(privObject.getFunctionName())
            .actionType(actionType)
            .className(privObject.getClassName())
            .build();
      }
      break;
    case DUMMYPARTITION:
    case PARTITION:
      Table tbl = privObject.getTable();
      List<String> col = tableName2Cols == null ? null :
              tableName2Cols.get(Table.getCompleteName(tbl.getDbName(), tbl.getTableName()));
      hivePrivObject = HivePrivilegeObject.builder(privObjType)
          .catName(catNameOrCurrent(tbl.getCatName()))
          .dbName(tbl.getDbName())
          .objectName(tbl.getTableName())
          .columns(col)
          .actionType(actionType)
          .ownerName(tbl.getOwner())
          .ownerType(tbl.getOwnerType())
          .build();
      break;
    case SERVICE_NAME:
      hivePrivObject = HivePrivilegeObject.builder(privObjType)
          .objectName(privObject.getServiceName())
          .actionType(actionType)
          .build();
      break;
    case DATACONNECTOR:
      DataConnector connector = privObject.getDataConnector();
      hivePrivObject = HivePrivilegeObject.builder(privObjType)
          .objectName(connector.getName())
          .actionType(actionType)
          .ownerName(connector.getOwnerName())
          .ownerType(connector.getOwnerType())
          .build();
      break;
    default:
      throw new AssertionError("Unexpected object type");
    }
    hivePrivObjs.add(hivePrivObject);
  }

}
