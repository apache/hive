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

package org.apache.hadoop.hive.ql.ddl.dataconnector.drop;

import org.apache.hadoop.hive.llap.LlapHiveUtils;
import org.apache.hadoop.hive.llap.ProactiveEviction;
import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;

/**
 * Operation process of dropping a data connector.
 */
public class DropDataConnectorOperation extends DDLOperation<DropDataConnectorDesc> {
  public DropDataConnectorOperation(DDLOperationContext context, DropDataConnectorDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    try {
      String dcName = desc.getConnectorName();
      ReplicationSpec replicationSpec = desc.getReplicationSpec();
      if (replicationSpec.isInReplicationScope()) {
        DataConnector connector = context.getDb().getDataConnector(dcName);
        if (connector == null || !replicationSpec.allowEventReplacementInto(connector.getParameters())) {
          return 0;
        }
      }

      context.getDb().dropDataConnector(dcName, desc.getIfExists());

      // TODO is this required for Connectors
      if (LlapHiveUtils.isLlapMode(context.getConf())) {
        ProactiveEviction.Request.Builder llapEvictRequestBuilder = ProactiveEviction.Request.Builder.create();
        llapEvictRequestBuilder.addDb(dcName);
        ProactiveEviction.evict(context.getConf(), llapEvictRequestBuilder.build());
      }
    } catch (NoSuchObjectException ex) {
      throw new HiveException(ex, ErrorMsg.DATACONNECTOR_NOT_EXISTS, desc.getConnectorName());
    }

    return 0;
  }
}
