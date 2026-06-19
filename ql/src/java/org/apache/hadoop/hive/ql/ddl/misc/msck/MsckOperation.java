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

package org.apache.hadoop.hive.ql.ddl.misc.msck;

import static org.apache.hadoop.hive.metastore.Msck.getProxyClass;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.Msck;
import org.apache.hadoop.hive.metastore.MsckInfo;
import org.apache.hadoop.hive.metastore.PartitionManagementTask;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.MetastoreException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.HiveTableName;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.thrift.TException;

/**
 * Operation process of metastore check.
 *
 * MetastoreCheck, see if the data in the metastore matches what is on the dfs. Current version checks for tables
 * and partitions that are either missing on disk on in the metastore.
 */
public class MsckOperation extends DDLOperation<MsckDesc> {
  public MsckOperation(DDLOperationContext context, MsckDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException, IOException, TException, MetastoreException {
    try {
      Msck msck = new Msck(false, false);
      msck.init(Msck.getMsckConf(context.getDb().getConf()));
      msck.updateExpressionProxy(getProxyClass(context.getDb().getConf()));

      TableName tableName = HiveTableName.of(desc.getTableName());

      long partitionExpirySeconds = -1L;
      try (HiveMetaStoreClient msc = new HiveMetaStoreClient(context.getConf())) {
        boolean msckEnablePartitionRetention = MetastoreConf.getBoolVar(context.getConf(),
            MetastoreConf.ConfVars.MSCK_REPAIR_ENABLE_PARTITION_RETENTION);
        if (msckEnablePartitionRetention) {
          Table table = msc.getTable(SessionState.get().getCurrentCatalog(), tableName.getDb(), tableName.getTable());
          String qualifiedTableName = Warehouse.getCatalogQualifiedTableName(table);
          partitionExpirySeconds = PartitionManagementTask.getRetentionPeriodInSeconds(table);
          LOG.info("{} - Retention period ({}s) for partition is enabled for MSCK REPAIR..", qualifiedTableName,
              partitionExpirySeconds);
        }
      }
      MsckInfo msckInfo = new MsckInfo(SessionState.get().getCurrentCatalog(), tableName.getDb(), tableName.getTable(),
          desc.getFilterExp(), desc.getResFile(), desc.isRepairPartitions(),
          desc.isAddPartitions(), desc.isDropPartitions(), partitionExpirySeconds);
      int result = msck.repair(msckInfo);
      Map<String, String> smallFilesStats = msckInfo.getSmallFilesStats();
      if (smallFilesStats != null && !smallFilesStats.isEmpty()) {
        // keep the small files information in logInfo
        List<String> logInfo = smallFilesStats.entrySet().stream()
                .map(entry -> String.format(
                        "Average file size is too small, small files exist. %n Partition name: %s. %s",
                        entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
        // print out the small files information on console to end users
        SessionState ss = SessionState.get();
        if (ss != null && ss.getConsole() != null) {
          ss.getConsole().printInfo("[MSCK] Small files detected.");
          ss.getConsole().printInfo(""); // add a blank line for separation
          logInfo.forEach(line -> ss.getConsole().printInfo("[MSCK] " + line));
        } else {
          // if there is no console to print out, keep the small files info in logs
          LOG.info("There are small files exist.\n{}", String.join("\n", logInfo));
        }
      }
      return result;
    } catch (MetaException | MetastoreException e) {
      LOG.error("Unable to create msck instance.", e);
      throw e;
    } catch (SemanticException e) {
      LOG.error("Msck failed.", e);
      return 1;
    }
  }

}
