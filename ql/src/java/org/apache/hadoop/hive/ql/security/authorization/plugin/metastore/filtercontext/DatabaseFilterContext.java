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

package org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.filtercontext;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.HiveMetaStoreAuthorizableEvent;
import org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.HiveMetaStoreAuthzInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DatabaseFilterContext extends HiveMetaStoreAuthorizableEvent {

  private static final Logger LOG = LoggerFactory.getLogger(DatabaseFilterContext.class);

  String catName = null;
  List<String> databaseNames = null;
  Map<String, Database> databaseMap = null;

  public DatabaseFilterContext(String catName, List<String> dbNames) {
    super(null);
    this.catName = catName;
    this.databaseNames = dbNames;
    getAuthzContext();
  }

  private DatabaseFilterContext(List<String> dbNames, Map<String, Database> dbMap) {
    super(null);
    this.databaseNames = dbNames != null ? dbNames : new ArrayList<>();
    this.databaseMap = dbMap != null ? dbMap : new HashMap<>();
    getAuthzContext();
  }

  public static DatabaseFilterContext createFromDatabases(List<Database> databases) {
    List<String> dbNames = new ArrayList<>();
    Map<String, Database> dbMap = new HashMap<>();

    if (databases != null) {
      for (Database db : databases) {
        if (db != null) {
          String dbName = db.getName();
          dbNames.add(dbName);
          dbMap.put(dbName, db);
        }
      }
    }

    return new DatabaseFilterContext(dbNames, dbMap);
  }

  @Override
  public HiveMetaStoreAuthzInfo getAuthzContext() {
    HiveMetaStoreAuthzInfo ret = new HiveMetaStoreAuthzInfo(preEventContext, HiveOperationType.QUERY, getInputHObjs(), getOutputHObjs(), null);
    return ret;
  }

  private List<HivePrivilegeObject> getInputHObjs() {
    LOG.debug("==> DatabaseFilterContext.getInputHObjs()");

    List<HivePrivilegeObject> ret = new ArrayList<>();
    HivePrivilegeObjectType type = HivePrivilegeObjectType.DATABASE;
    for (String dbName : databaseNames) {
      Database db = (databaseMap != null) ? databaseMap.get(dbName) : null;
      if (db != null) {
        ret.add(getHivePrivilegeObject(db));
      } else {
        HivePrivilegeObject hivePrivilegeObject = new HivePrivilegeObject(type, dbName);
        ret.add(hivePrivilegeObject);
      }
    }

    LOG.debug("<== DatabaseFilterContext.getInputHObjs(): ret=" + ret);

    return ret;
  }

  private List<HivePrivilegeObject> getOutputHObjs() {
    return Collections.emptyList();
  }

  public List<String> getDatabaseNames() {
    return databaseNames;
  }
}