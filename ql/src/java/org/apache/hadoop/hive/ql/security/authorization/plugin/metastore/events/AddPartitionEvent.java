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
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.PreAddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.HiveMetaStoreAuthorizableEvent;
import org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.HiveMetaStoreAuthzInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/*
 Authorizable Event for HiveMetaStore operation  AddPartition
 */

public class AddPartitionEvent extends HiveMetaStoreAuthorizableEvent {
  private static final Logger LOG = LoggerFactory.getLogger(AddPartitionEvent.class);

  private String COMMAND_STR = "alter table %s add partition %s";

  public AddPartitionEvent(PreEventContext preEventContext) {
    super(preEventContext);
  }

  @Override
  public HiveMetaStoreAuthzInfo getAuthzContext() {
    HiveMetaStoreAuthzInfo ret = new HiveMetaStoreAuthzInfo(preEventContext, HiveOperationType.ALTERTABLE_ADDPARTS, getInputHObjs(), getOutputHObjs(), COMMAND_STR);

    return ret;
  }

  private List<HivePrivilegeObject> getInputHObjs() {
    return Collections.emptyList();
  }

  private List<HivePrivilegeObject> getOutputHObjs() {
    LOG.debug("==> AddPartitionEvent.getOutputHObjs()");

    List<HivePrivilegeObject> ret   = new ArrayList<>();
    PreAddPartitionEvent      event = (PreAddPartitionEvent) preEventContext;
    Table                     table = event.getTable();

    ret.add(getHivePrivilegeObject(table));

    List<Partition> partitions = event.getPartitions();

    if (partitions != null) {
      for (Partition partition : partitions) {
        String uri = getSdLocation(partition.getSd());

        if (StringUtils.isNotEmpty(uri)) {
          ret.add(getHivePrivilegeObjectDfsUri(uri));
        }
      }
    }

    COMMAND_STR = buildCommandString(COMMAND_STR, table);

    LOG.debug("<== AddPartitionEvent.getOutputHObjs(): ret={}", ret);

    return ret;
  }

  private String buildCommandString(String cmdStr, Table tbl) {
    String ret = cmdStr;

    if (tbl != null) {
      String tblName     = (StringUtils.isNotEmpty(tbl.getTableName())? " " + tbl.getTableName() : "");

      StringBuilder partitions  = new StringBuilder();
      List<FieldSchema> fieldSchemas = tbl.getPartitionKeys();
      for (FieldSchema fieldSchema : fieldSchemas) {
         partitions.append(" ");
         partitions.append(fieldSchema.getName());
      }

      ret = String.format(cmdStr, tblName, partitions.toString());
    }
    return ret;
  }
}