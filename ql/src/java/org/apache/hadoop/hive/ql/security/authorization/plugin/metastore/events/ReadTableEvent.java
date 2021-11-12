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

import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.apache.hadoop.hive.metastore.events.PreReadTableEvent;
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

public class ReadTableEvent extends HiveMetaStoreAuthorizableEvent {
  private static final Logger LOG = LoggerFactory.getLogger(ReadTableEvent.class);
  private static final String COMMAND_STR = "select";

  public ReadTableEvent(PreEventContext preEventContext) {
    super(preEventContext);
  }

  @Override public HiveMetaStoreAuthzInfo getAuthzContext() {
    HiveMetaStoreAuthzInfo ret =
        new HiveMetaStoreAuthzInfo(preEventContext, HiveOperationType.QUERY, getInputHObjs(), getOutputHObjs(),
            COMMAND_STR);
    return ret;
  }

  private List<HivePrivilegeObject> getInputHObjs() {
    LOG.debug("==> ReadTableEvent.getInputHObjs()");

    List<HivePrivilegeObject> ret = new ArrayList<>();
    PreReadTableEvent preReadTableEvent = (PreReadTableEvent) preEventContext;
    String dbName = preReadTableEvent.getTable().getDbName();
    Table table = preReadTableEvent.getTable();

    ret.add(getHivePrivilegeObject(table));

    LOG.debug("<== ReadTableEvent.getInputHObjs()" + ret);

    return ret;
  }

  private List<HivePrivilegeObject> getOutputHObjs() {
    return Collections.emptyList();
  }

}