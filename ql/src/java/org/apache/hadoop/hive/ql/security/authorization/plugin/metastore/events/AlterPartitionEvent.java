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
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.events.PreAlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.HiveMetaStoreAuthorizableEvent;
import org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.HiveMetaStoreAuthzInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/*
 Authorizable Event for HiveMetaStore operation  AlterPartition
 */

public class AlterPartitionEvent extends HiveMetaStoreAuthorizableEvent {
  private static final Logger LOG = LoggerFactory.getLogger(AlterPartitionEvent.class);

  private String COMMAND_STR = "alter table %s partition %s";

  public AlterPartitionEvent(PreEventContext preEventContext) {
    super(preEventContext);
  }

  @Override
  public HiveMetaStoreAuthzInfo getAuthzContext() {
    HiveMetaStoreAuthzInfo ret = new HiveMetaStoreAuthzInfo(preEventContext, HiveOperationType.ALTERPARTITION_FILEFORMAT, getInputHObjs(), getOutputHObjs(), COMMAND_STR);

    return ret;
  }

  private List<HivePrivilegeObject> getInputHObjs() {
    LOG.debug("==> AlterPartitionEvent.getInputHObjs()");

    List<HivePrivilegeObject> ret   = new ArrayList<>();
    PreAlterPartitionEvent    event = (PreAlterPartitionEvent) preEventContext;

    ret.add(getHivePrivilegeObject(event.getTable()));

    LOG.debug("<== AlterPartitionEvent.getInputHObjs() ret={}", ret);

    return ret;
  }

  private List<HivePrivilegeObject> getOutputHObjs() {
    LOG.debug("==> AlterPartitionEvent.getOutputHObjs()");

    List<HivePrivilegeObject> ret   = new ArrayList<>();
    PreAlterPartitionEvent    event = (PreAlterPartitionEvent) preEventContext;

    ret.add(getHivePrivilegeObject(event.getTable()));

    Partition newPartition = event.getNewPartition();
    String    newUri       = (newPartition != null) ? getSdLocation(newPartition.getSd()) : "";

    if (StringUtils.isNotEmpty(newUri)) {
        ret.add(getHivePrivilegeObjectDfsUri(newUri));
    }

    COMMAND_STR = buildCommandString(COMMAND_STR, event.getTableName(), newPartition);

    LOG.debug("<== AlterPartitionEvent.getOutputHObjs() ret={}", ret);

    return ret;
  }

  private String buildCommandString(String cmdStr, String tbl, Partition partition ) {
    String ret = cmdStr;

    if (tbl != null) {
      String tblName    = (StringUtils.isNotEmpty(tbl) ? " " + tbl : "");
      String partitionStr = (partition != null) ? partition.toString() : "";
      ret               = String.format(cmdStr, tblName, partitionStr);
    }

    return ret;
  }
}
