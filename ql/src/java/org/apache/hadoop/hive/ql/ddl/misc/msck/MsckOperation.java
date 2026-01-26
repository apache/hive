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

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.Msck;
import org.apache.hadoop.hive.metastore.MsckInfo;
import org.apache.hadoop.hive.metastore.PartitionManagementTask;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.MetastoreException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.HiveTableName;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Operation process of metastore check.
 *
 * MetastoreCheck, see if the data in the metastore matches what is on the dfs. Current version checks for tables
 * and partitions that are either missing on disk on in the metastore.
 */
public class MsckOperation extends DDLOperation<MsckDesc> {

  private static final Logger LOG = LoggerFactory.getLogger(MsckOperation.class);
  private static final SessionState.LogHelper CONSOLE = SessionState.getConsole();

  public MsckOperation(DDLOperationContext context, MsckDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException, IOException, TException, MetastoreException {
    Table table = context.getDb().getTable(desc.getTableName());

    if (DDLUtils.isIcebergTable(table)) {
      MsckResult result =
          table.getStorageHandler().repair(table, context.getConf(), desc);

      CONSOLE.printInfo(result.message());

      // Print details (file paths) if available
      if (!result.details().isEmpty()) {
        String filesLog = formatFilesForConsole(result.details());
        CONSOLE.printInfo("[MSCK] Details: " + filesLog);
      }

      return 0;
    }

    try {
      Msck msck = new Msck(false, false);
      msck.init(Msck.getMsckConf(context.getDb().getConf()));
      msck.updateExpressionProxy(getProxyClass(context.getDb().getConf()));

      TableName tableName = HiveTableName.of(desc.getTableName());

      long partitionExpirySeconds = -1L;
      boolean msckEnablePartitionRetention = MetastoreConf.getBoolVar(context.getConf(),
          MetastoreConf.ConfVars.MSCK_REPAIR_ENABLE_PARTITION_RETENTION);
      if (msckEnablePartitionRetention) {
        org.apache.hadoop.hive.metastore.api.Table tTable = table.getTTable();
        String qualifiedTableName = Warehouse.getCatalogQualifiedTableName(tTable);
        partitionExpirySeconds = PartitionManagementTask.getRetentionPeriodInSeconds(tTable);
        LOG.info("{} - Retention period ({}s) for partition is enabled for MSCK REPAIR..", qualifiedTableName,
            partitionExpirySeconds);
      }
      MsckInfo msckInfo = new MsckInfo(SessionState.get().getCurrentCatalog(), tableName.getDb(), tableName.getTable(),
          desc.getFilterExp(), desc.getResFile(), desc.isRepair(),
          desc.isAddPartitions(), desc.isDropPartitions(), partitionExpirySeconds);
      int result = msck.repair(msckInfo);
      Map<String, String> smallFilesStats = msckInfo.getSmallFilesStats();
      if (smallFilesStats != null && !smallFilesStats.isEmpty()) {
        // keep the small files information in logInfo
        List<String> logInfo = smallFilesStats.entrySet().stream()
            .map(entry -> String.format(
                "Average file size is too small, small files exist. %n Partition name: %s. %s",
                entry.getKey(), entry.getValue()))
            .toList();
        // print out the small files information on console to end users
        CONSOLE.printInfo("[MSCK] Small files detected.\n");
        logInfo.forEach(line -> CONSOLE.printInfo("[MSCK] " + line));
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

  /**
   * Format list of files for console output.
   */
  private static String formatFilesForConsole(List<String> files) {
    int numPathsToLog = LOG.isTraceEnabled() ? 100 : 3;
    int total = files.size();
    String fileNames = Joiner.on(", ").join(Iterables.limit(files, numPathsToLog));

    if (total > numPathsToLog) {
      int remaining = total - numPathsToLog;
      fileNames += String.format(" (and %d more)", remaining);
    }
    return fileNames;
  }

}

