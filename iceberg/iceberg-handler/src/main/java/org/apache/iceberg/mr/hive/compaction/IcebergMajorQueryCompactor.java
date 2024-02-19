/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive.compaction;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.Context.RewritePolicy;
import org.apache.hadoop.hive.ql.DriverUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.txn.compactor.CompactorContext;
import org.apache.hadoop.hive.ql.txn.compactor.QueryCompactor;
import org.apache.hive.iceberg.org.apache.orc.storage.common.TableName;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.Table;
import org.apache.iceberg.mr.hive.IcebergTableUtil;
import org.apache.iceberg.types.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergMajorQueryCompactor extends QueryCompactor  {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergMajorQueryCompactor.class.getName());

  @Override
  public boolean run(CompactorContext context) throws IOException, HiveException, InterruptedException {

    String compactTableName = TableName.getDbTable(context.getTable().getDbName(), context.getTable().getTableName());
    Map<String, String> tblProperties = context.getTable().getParameters();
    LOG.debug("Initiating compaction for the {} table", compactTableName);

    String partSpec = context.getCompactionInfo().partName;
    String compactionQuery;
    RewritePolicy rewritePolicy;

    if (partSpec == null) {
      compactionQuery = String.format("insert overwrite table %s select * from %<s",
          compactTableName);
      rewritePolicy = RewritePolicy.ALL_PARTITIONS;
    } else {
      Table table = IcebergTableUtil.getTable(context.getConf(), context.getTable());
      PartitionData partitionData = DataFiles.data(table.spec(), partSpec);
      context.getConf().set(Context.compactPartition, partSpec);
      compactionQuery = String.format("insert overwrite table %1$s partition(%2$s) select * from %1$s where %3$s",
          compactTableName, partDataToSQL(partitionData, partSpec, ","),
          partDataToSQL(partitionData, partSpec, " and "));
      rewritePolicy = RewritePolicy.SINGLE_PARTITION;
    }

    SessionState sessionState = setupQueryCompactionSession(context.getConf(),
        context.getCompactionInfo(), tblProperties);
    HiveConf.setVar(context.getConf(), ConfVars.REWRITE_POLICY, rewritePolicy.name());
    try {
      DriverUtils.runOnDriver(context.getConf(), sessionState, compactionQuery);
      LOG.info("Completed compaction for table {}", compactTableName);
    } catch (HiveException e) {
      LOG.error("Error doing query based {} compaction", rewritePolicy.name(), e);
      throw new RuntimeException(e);
    } finally {
      sessionState.setCompaction(false);
    }

    return true;
  }

  private String partDataToSQL(PartitionData partitionData, String partSpec, String delimiter) {
    StringBuilder sb = new StringBuilder();
    List<String> values = Arrays
        .stream(partSpec.split("/"))
        .map(x -> x.split("=")[1])
        .collect(Collectors.toList());

    for (int i = 0; i < partitionData.size(); ++i) {
      if (i > 0) {
        sb.append(delimiter);
      }

      String quoteOpt = "";
      if (partitionData.getType(i).typeId() == Type.TypeID.STRING ||
          partitionData.getType(i).typeId() == Type.TypeID.DATE ||
          partitionData.getType(i).typeId() == Type.TypeID.TIME ||
          partitionData.getType(i).typeId() == Type.TypeID.TIMESTAMP ||
          partitionData.getType(i).typeId() == Type.TypeID.BINARY) {
        quoteOpt = "'";
      }

      sb.append(partitionData.getSchema().getFields().get(i).name())
          .append("=")
          .append(quoteOpt)
          .append(values.get(i))
           .append(quoteOpt);
    }

    return sb.toString();
  }
}
