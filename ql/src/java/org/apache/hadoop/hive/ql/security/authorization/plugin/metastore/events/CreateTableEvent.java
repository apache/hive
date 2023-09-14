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
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.events.PreCreateTableEvent;
import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.HiveMetaStoreAuthorizableEvent;
import org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.HiveMetaStoreAuthzInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/*
 Authorizable Event for HiveMetaStore operation CreateTable
 */

public class CreateTableEvent extends HiveMetaStoreAuthorizableEvent {
  private static final Logger LOG = LoggerFactory.getLogger(CreateTableEvent.class);

  private String COMMAND_STR = "create table";


  public CreateTableEvent(PreEventContext preEventContext) {
      super(preEventContext);
  }

  @Override
  public HiveMetaStoreAuthzInfo getAuthzContext() {
    HiveMetaStoreAuthzInfo ret = new HiveMetaStoreAuthzInfo(preEventContext, HiveOperationType.CREATETABLE, getInputHObjs(), getOutputHObjs(), COMMAND_STR);

    return ret;
  }

  private List<HivePrivilegeObject> getInputHObjs() {
    List<HivePrivilegeObject> ret   = new ArrayList<>();
    PreCreateTableEvent       event = (PreCreateTableEvent) preEventContext;
    Table                     table = event.getTable();
    String                    uri   = getSdLocation(table.getSd());

    if (StringUtils.isNotEmpty(uri)) {
      ret.add(new HivePrivilegeObject(HivePrivilegeObjectType.DFS_URI, null, uri));
    }
    return ret;
  }

  private List<HivePrivilegeObject> getOutputHObjs() {
    LOG.debug("==> CreateTableEvent.getOutputHObjs()");

    List<HivePrivilegeObject> ret   = new ArrayList<>();
    PreCreateTableEvent       event = (PreCreateTableEvent) preEventContext;
    Table                     table = event.getTable();
    Database                  database = event.getDatabase();
    String                    uri   = getSdLocation(table.getSd());

    ret.add(getHivePrivilegeObject(database));
    ret.add(getHivePrivilegeObject(table));

    if (StringUtils.isNotEmpty(uri) && table.getTableType() != TableType.EXTERNAL_TABLE.toString()) {
      ret.add(new HivePrivilegeObject(HivePrivilegeObjectType.DFS_URI, null, uri));
    }

    COMMAND_STR = buildCommandString(COMMAND_STR,table);

    LOG.debug("<== CreateTableEvent.getOutputHObjs(): ret={}", ret);

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
