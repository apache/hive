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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.metastore.events.PreDropDataConnectorEvent;
import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.HiveMetaStoreAuthorizableEvent;
import org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.HiveMetaStoreAuthzInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 Authorizable Event for HiveMetaStore operation DropDataConnector
 */

public class DropDataConnectorEvent extends HiveMetaStoreAuthorizableEvent {
  private static final Logger LOG = LoggerFactory.getLogger(DropDataConnectorEvent.class);

  private String COMMAND_STR = "drop connector";

  public DropDataConnectorEvent(PreEventContext preEventContext) {
    super(preEventContext);
  }

  @Override
  public HiveMetaStoreAuthzInfo getAuthzContext() {
    HiveMetaStoreAuthzInfo ret =
        new HiveMetaStoreAuthzInfo(preEventContext, HiveOperationType.DROPDATACONNECTOR, getInputHObjs(),
            getOutputHObjs(), COMMAND_STR);

    return ret;
  }

  private List<HivePrivilegeObject> getInputHObjs() {
    LOG.debug("==> DropDataConnectorEvent.getInputHObjs()");

    List<HivePrivilegeObject> ret = new ArrayList<>();
    PreDropDataConnectorEvent event = (PreDropDataConnectorEvent) preEventContext;
    DataConnector connector = event.getDataConnector();

    ret.add(getHivePrivilegeObject(connector));

    COMMAND_STR = buildCommandString(COMMAND_STR, connector);

    LOG.debug("<== DropDataConnectorEvent.getInputHObjs(): ret={}", ret);

    return ret;
  }

  private List<HivePrivilegeObject> getOutputHObjs() {
    return getInputHObjs();
  }

  private String buildCommandString(String cmdStr, DataConnector connector) {
    String ret = cmdStr;

    if (connector != null) {
      String dcName = connector.getName();
      ret = ret + (StringUtils.isNotEmpty(dcName) ? " " + dcName : "");
    }

    return ret;
  }
}
