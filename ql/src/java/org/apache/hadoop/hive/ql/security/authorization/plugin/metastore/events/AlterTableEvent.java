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

package org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.events;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.events.PreAlterTableEvent;
import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.apache.hadoop.hive.ql.security.authorization.HiveCustomStorageHandlerUtils;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.HiveMetaStoreAuthorizableEvent;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivObjectActionType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.HiveMetaStoreAuthzInfo;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStorageAuthorizationHandler;
import org.apache.hadoop.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

/*
 Authorizable Event for HiveMetaStore operation  AlterTableEvent
 */

public class AlterTableEvent extends HiveMetaStoreAuthorizableEvent {
  private static final Log LOG = LogFactory.getLog(AlterTableEvent.class);

  private String COMMAND_STR = "alter table";

  public AlterTableEvent(PreEventContext preEventContext) {
    super(preEventContext);
  }

  @Override
  public HiveMetaStoreAuthzInfo getAuthzContext() {
    HiveMetaStoreAuthzInfo ret = new HiveMetaStoreAuthzInfo(preEventContext, getOperationType(), getInputHObjs(), getOutputHObjs(), COMMAND_STR);

    return ret;
  }

  private HiveOperationType getOperationType() {
    PreAlterTableEvent event    = (PreAlterTableEvent) preEventContext;
    Table              table    = event.getNewTable();
    Table              oldTable = event.getOldTable();
    String             newUri   = (table != null) ? getSdLocation(table.getSd()) : "";
    String             oldUri   = (oldTable != null) ? getSdLocation(oldTable.getSd()) : "";

    return StringUtils.equals(oldUri, newUri) ? HiveOperationType.ALTERTABLE_ADDCOLS : HiveOperationType.ALTERTABLE_LOCATION;
  }

  private List<HivePrivilegeObject> getInputHObjs() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("==> AlterTableEvent.getInputHObjs()");
    }

    List<HivePrivilegeObject> ret      = new ArrayList<>();
    PreAlterTableEvent        event    = (PreAlterTableEvent) preEventContext;
    Table                     oldTable = event.getOldTable();

    ret.add(getHivePrivilegeObject(oldTable));

    COMMAND_STR = buildCommandString(COMMAND_STR, oldTable);

    if (LOG.isDebugEnabled()) {
      LOG.debug("<== AlterTableEvent.getInputHObjs(): ret=" + ret);
    }

    return ret;
  }

  private List<HivePrivilegeObject> getOutputHObjs() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("==> AlterTableEvent.getOutputHObjs()");
    }

    List<HivePrivilegeObject> ret      = new ArrayList<>();
    PreAlterTableEvent event    = (PreAlterTableEvent) preEventContext;
    Table newTable = event.getNewTable();

    ret.add(getHivePrivilegeObject(newTable));
    Table oldTable = event.getOldTable();
    String oldUri   = (oldTable != null) ? getSdLocation(oldTable.getSd()) : "";
    String newUri   = getSdLocation(newTable.getSd());

    if (!StringUtils.equals(oldUri, newUri)) {
      ret.add(getHivePrivilegeObjectDfsUri(newUri));
    }

    if(newTable.getParameters().containsKey(hive_metastoreConstants.META_TABLE_STORAGE)) {
      String storageUri = "";
      DefaultStorageHandler defaultStorageHandler = null;
      HiveStorageHandler hiveStorageHandler = null;
      Configuration conf = new Configuration();
      Map<String, String> tableProperties = new HashMap<>();
      tableProperties.putAll(newTable.getSd().getSerdeInfo().getParameters());
      tableProperties.putAll(newTable.getParameters());
      try {
        hiveStorageHandler = (HiveStorageHandler) ReflectionUtils.newInstance(
                conf.getClassByName(newTable.getParameters().get(hive_metastoreConstants.META_TABLE_STORAGE)), event.getHandler().getConf());
        Method methodIsImplemented = hiveStorageHandler.getClass().getMethod("getURIForAuth", Map.class);
        if(methodIsImplemented != null && hiveStorageHandler instanceof DefaultStorageHandler) {
          DefaultStorageHandler defaultHandler = (DefaultStorageHandler) ReflectionUtils.newInstance(
                  conf.getClassByName(newTable.getParameters().get(hive_metastoreConstants.META_TABLE_STORAGE)), event.getHandler().getConf());
          storageUri = defaultHandler.getURIForAuth(tableProperties).toString();
        }else if(methodIsImplemented != null && hiveStorageHandler instanceof HiveStorageAuthorizationHandler){
          HiveStorageAuthorizationHandler authorizationHandler = (HiveStorageAuthorizationHandler) ReflectionUtils.newInstance(
                  conf.getClassByName(newTable.getParameters().get(hive_metastoreConstants.META_TABLE_STORAGE)), event.getHandler().getConf());
          storageUri = authorizationHandler.getURIForAuth(tableProperties).toString();
        }
      }catch(Exception ex){
        //Custom storage handler that has not implemented the getURIForAuth()
        storageUri = hiveStorageHandler.getClass().getName()+"://"+
                HiveCustomStorageHandlerUtils.getTablePropsForCustomStorageHandler(tableProperties);
      }
      ret.add(new HivePrivilegeObject(HivePrivilegeObjectType.STORAGEHANDLER_URI, null, storageUri, null, null,
              HivePrivObjectActionType.OTHER, null, newTable.getParameters().get(hive_metastoreConstants.META_TABLE_STORAGE), newTable.getOwner(), newTable.getOwnerType()));
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("<== AlterTableEvent.getOutputHObjs(): ret=" + ret);
    }

    return ret;
  }

  private String buildCommandString(String cmdStr, Table tbl) {
    String ret = cmdStr;
    if (tbl != null) {
      String tblName = tbl.getTableName();
      ret            = ret + (StringUtils.isNotEmpty(tblName)? " " + tblName : "");
    }
    return ret;
  }
}
