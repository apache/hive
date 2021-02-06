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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.PreDropTableEvent;
import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStorageAuthorizationHandler;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.HiveMetaStoreAuthorizableEvent;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.HiveMetaStoreAuthzInfo;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

/*
 Authorizable Event for HiveMetaStore operation DropTable
 */

public class DropTableEvent extends HiveMetaStoreAuthorizableEvent {
  private static final Log LOG = LogFactory.getLog(DropTableEvent.class);

  private String COMMAND_STR = "Drop table";

  public DropTableEvent(PreEventContext preEventContext) {
    super(preEventContext);
  }

  @Override
  public HiveMetaStoreAuthzInfo getAuthzContext() {
    HiveMetaStoreAuthzInfo ret = new HiveMetaStoreAuthzInfo(preEventContext, HiveOperationType.DROPTABLE, getInputHObjs(), getOutputHObjs(), COMMAND_STR);

    return ret;
  }

  private List<HivePrivilegeObject> getInputHObjs() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("==> DropTableEvent.getInputHObjs()");
    }

    List<HivePrivilegeObject> ret   = new ArrayList<>();
    PreDropTableEvent         event = (PreDropTableEvent) preEventContext;
    Table                     table = event.getTable();
    ret.add(getHivePrivilegeObject(table));

    if(table.getParameters().containsKey(hive_metastoreConstants.META_TABLE_STORAGE)) {
      String storageUri = "";
      DefaultStorageHandler defaultStorageHandler = null;
      HiveStorageHandler hiveStorageHandler = null;
      Configuration conf = new Configuration();
      Map<String, String> tableProperties = new HashMap<>();
      tableProperties.putAll(table.getSd().getSerdeInfo().getParameters());
      tableProperties.putAll(table.getParameters());
      try {
        hiveStorageHandler = (HiveStorageHandler) ReflectionUtils.newInstance(
                conf.getClassByName(table.getParameters().get(hive_metastoreConstants.META_TABLE_STORAGE)), event.getHandler().getConf());
        Method methodIsImplemented = hiveStorageHandler.getClass().getMethod("getURIForAuth", Map.class);
        if(methodIsImplemented != null && hiveStorageHandler instanceof DefaultStorageHandler) {
          DefaultStorageHandler defaultHandler = (DefaultStorageHandler) ReflectionUtils.newInstance(
                  conf.getClassByName(table.getParameters().get(hive_metastoreConstants.META_TABLE_STORAGE)), event.getHandler().getConf());
          storageUri = defaultHandler.getURIForAuth(tableProperties).toString();
        }else if(methodIsImplemented != null && hiveStorageHandler instanceof HiveStorageAuthorizationHandler){
          HiveStorageAuthorizationHandler authorizationHandler = (HiveStorageAuthorizationHandler) ReflectionUtils.newInstance(
                  conf.getClassByName(table.getParameters().get(hive_metastoreConstants.META_TABLE_STORAGE)), event.getHandler().getConf());
          storageUri = authorizationHandler.getURIForAuth(tableProperties).toString();
        }
      }catch(Exception ex){
        //Custom storage handler that has not implemented the getURIForAuth()
        storageUri = hiveStorageHandler.getClass().getName()+"://"+
                getTablePropsForCustomStorageHandler(tableProperties);
      }
      ret.add(new HivePrivilegeObject(HivePrivilegeObjectType.STORAGEHANDLER_URI, null, storageUri, null, null,
              null, null, table.getParameters().get(hive_metastoreConstants.META_TABLE_STORAGE)));
    }

    COMMAND_STR = buildCommandString(COMMAND_STR, table);

    if (LOG.isDebugEnabled()) {
      LOG.debug("<== DropTableEvent.getInputHObjs(): ret=" + ret);
    }

    return ret;
  }

  private List<HivePrivilegeObject> getOutputHObjs() { return Collections.emptyList(); }

  private String buildCommandString(String cmdStr, Table tbl) {
    String ret = cmdStr;
    if (tbl != null) {
      String tblName = tbl.getTableName();
      ret            = ret + (StringUtils.isNotEmpty(tblName)? " " + tblName : "");
    }
    return ret;
  }

  private static String getTablePropsForCustomStorageHandler(Map<String, String> tableProperties){
    StringBuilder properties = new StringBuilder();
    for(Map.Entry<String,String> serdeMap : tableProperties.entrySet()){
      properties.append(serdeMap.getValue());
      properties.append("/");
    }
    return properties.toString();
  }
}
