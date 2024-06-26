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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context.RewritePolicy;
import org.apache.hadoop.hive.ql.DriverUtils;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.txn.compactor.CompactorContext;
import org.apache.hadoop.hive.ql.txn.compactor.QueryCompactor;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hive.iceberg.org.apache.orc.storage.common.TableName;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.Table;
import org.apache.iceberg.mr.hive.IcebergTableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergMajorQueryCompactor extends QueryCompactor  {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergMajorQueryCompactor.class.getName());

  @Override
  public boolean run(CompactorContext context) throws IOException, HiveException, InterruptedException {

    String compactTableName = TableName.getDbTable(context.getTable().getDbName(), context.getTable().getTableName());
    Map<String, String> tblProperties = context.getTable().getParameters();
    LOG.debug("Initiating compaction for the {} table", compactTableName);

    HiveConf conf = new HiveConf(context.getConf());
    String partSpec = context.getCompactionInfo().partName;
    String compactionQuery;

    if (partSpec == null) {
      HiveConf.setVar(conf, ConfVars.REWRITE_POLICY, RewritePolicy.ALL_PARTITIONS.name());
      compactionQuery = String.format("insert overwrite table %s select * from %<s", compactTableName);
    } else {
      org.apache.hadoop.hive.ql.metadata.Table table = Hive.get(conf).getTable(context.getTable().getDbName(),
          context.getTable().getTableName());
      Map<String, String> partSpecMap = new LinkedHashMap<>();
      Warehouse.makeSpecFromName(partSpecMap, new Path(partSpec), null);

      Table icebergTable = IcebergTableUtil.getTable(conf, table.getTTable());
      Map<PartitionData, Integer> partitionInfo = IcebergTableUtil
          .getPartitionInfo(icebergTable, partSpecMap, false);
      Optional<Integer> specId = partitionInfo.values().stream().findFirst();

      if (!specId.isPresent()) {
        throw new HiveException(ErrorMsg.INVALID_PARTITION_SPEC);
      }

      if (partitionInfo.size() > 1) {
        throw new HiveException(ErrorMsg.TOO_MANY_COMPACTION_PARTITIONS);
      }

      HiveConf.setVar(conf, ConfVars.REWRITE_POLICY, RewritePolicy.PARTITION.name());
      conf.set(IcebergCompactionService.PARTITION_SPEC_ID, String.valueOf(specId.get()));
      conf.set(IcebergCompactionService.PARTITION_PATH, new Path(partSpec).toString());

      List<FieldSchema> partitionKeys = IcebergTableUtil.getPartitionKeys(icebergTable, specId.get());
      List<String> partValues = partitionKeys.stream().map(
          fs -> String.join("=", HiveUtils.unparseIdentifier(fs.getName()),
              TypeInfoUtils.convertStringToLiteralForSQL(partSpecMap.get(fs.getName()),
                  ((PrimitiveTypeInfo) TypeInfoUtils.getTypeInfoFromTypeString(fs.getType())).getPrimitiveCategory())
          )
      ).collect(Collectors.toList());

      String queryFields = table.getCols().stream()
          .map(FieldSchema::getName)
          .filter(col -> !partSpecMap.containsKey(col))
          .collect(Collectors.joining(","));

      compactionQuery = String.format("insert overwrite table %1$s partition(%2$s) " +
              "select %4$s from %1$s where %3$s and %6$s = %5$d",
          compactTableName,
          StringUtils.join(partValues, ","),
          StringUtils.join(partValues, " and "),
          queryFields, specId.get(), VirtualColumn.PARTITION_SPEC_ID.getName());
    }

    SessionState sessionState = setupQueryCompactionSession(conf, context.getCompactionInfo(), tblProperties);
    String compactionTarget = "table " + HiveUtils.unparseIdentifier(compactTableName) +
        (partSpec != null ? ", partition " + HiveUtils.unparseIdentifier(partSpec) : "");

    try {
      DriverUtils.runOnDriver(conf, sessionState, compactionQuery);
      LOG.info("Completed compaction for {}", compactionTarget);
      return true;
    } catch (HiveException e) {
      LOG.error("Failed compacting {}", compactionTarget, e);
      throw e;
    } finally {
      sessionState.setCompaction(false);
    }
  }
}
