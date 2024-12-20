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

package org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.events;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.metastore.events.PreAlterDataConnectorEvent;
import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.HiveMetaStoreAuthorizableEvent;
import org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.HiveMetaStoreAuthzInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 Authorizable Event for HiveMetaStore operation AlterDataConnector
 */

public class AlterDataConnectorEvent extends HiveMetaStoreAuthorizableEvent {
  private static final Logger LOG = LoggerFactory.getLogger(AlterDataConnectorEvent.class);

  private String COMMAND_STR = "alter connector";

  public AlterDataConnectorEvent(PreEventContext preEventContext) {
    super(preEventContext);
  }

  @Override
  public HiveMetaStoreAuthzInfo getAuthzContext() {
    HiveMetaStoreAuthzInfo ret =
        new HiveMetaStoreAuthzInfo(preEventContext, HiveOperationType.ALTERDATACONNECTOR, getInputHObjs(),
            getOutputHObjs(), COMMAND_STR);

    return ret;
  }

  private List<HivePrivilegeObject> getInputHObjs() {
    LOG.debug("==> AlterDataConnectorEvent.getInputHObjs()");

    List<HivePrivilegeObject> ret = new ArrayList<>();
    PreAlterDataConnectorEvent event = (PreAlterDataConnectorEvent) preEventContext;
    DataConnector connector = event.getOldDataConnector();

    if (connector != null) {
      ret.add(getHivePrivilegeObject(connector));
      COMMAND_STR = buildCommandString(COMMAND_STR, connector);
      LOG.debug("<== AlterDataConnectorEvent.getInputHObjs(): ret={}", ret);
    }

    return ret;
  }

  private List<HivePrivilegeObject> getOutputHObjs() {
    LOG.debug("==> AlterDataConnectorEvent.getOutputHObjs()");

    List<HivePrivilegeObject> ret = new ArrayList<>();
    PreAlterDataConnectorEvent event = (PreAlterDataConnectorEvent) preEventContext;
    DataConnector connector = event.getNewDataConnector();

    if (connector != null) {
      ret.add(getHivePrivilegeObject(connector));
      COMMAND_STR = buildCommandString(COMMAND_STR, connector);
      LOG.debug("<== AlterDataConnectorEvent.getOutputHObjs(): ret={}", ret);
    }

    return ret;
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
