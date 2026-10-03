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

package org.apache.iceberg.mr.hive;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.ObjectCacheFactory;
import org.apache.hadoop.hive.ql.security.authorization.HiveCustomStorageHandlerUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.hive.writer.WriterBuilder;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializationUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;

/**
 * The DV writers of a query load the rewritable deletes planned for the job once per daemon.
 */
public class TestHiveIcebergRewritableDeletes {
  private static final String QUERY_ID = "rewritable_deletes_query";
  private static final Schema SCHEMA = new Schema(optional(1, "id", Types.LongType.get()));

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @After
  public void after() {
    ObjectCacheFactory.removeLlapQueryCache(QUERY_ID);
    LlapProxy.setDaemon(false);
  }

  private static JobConf jobConf(Table table, String location, int task) {
    JobConf jc = new JobConf();
    jc.set(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.varname, "tez");
    jc.set(HiveConf.ConfVars.HIVE_QUERY_ID.varname, QUERY_ID);
    jc.set(hive_metastoreConstants.META_TABLE_NAME, table.name());
    jc.set(Catalogs.NAME, table.name());
    jc.set(InputFormatConfig.SERIALIZED_TABLE_PREFIX + table.name(), SerializationUtil.serializeToBase64(table));
    HiveCustomStorageHandlerUtils.setWriteOperation(jc, table.name(), Context.Operation.DELETE);
    if (location != null) {
      jc.set(InputFormatConfig.REWRITABLE_DELETES_PREFIX + table.name(), location);
    }
    jc.set("mapred.task.id", new TaskAttemptID(new JobID("test", 0).getJtIdentifier(), 0, TaskType.REDUCE, task, 0)
        .toString());
    return jc;
  }

  @Test
  public void testLoadedOncePerDaemon() throws IOException {
    File dir = temp.newFolder();
    Assert.assertTrue(dir.delete());
    Table table = new HadoopTables().create(SCHEMA, PartitionSpec.unpartitioned(),
        ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"), dir.getAbsolutePath());
    String location = HiveTableUtil.rewritableDeletesLocation(table.location(), jobConf(table, null, 0));
    WriterBuilder.writeRewritableDeletes(table, null, location);

    LlapProxy.setDaemon(true);
    HiveIcebergOutputFormat outputFormat = new HiveIcebergOutputFormat();
    outputFormat.getHiveRecordWriter(jobConf(table, location, 0), null, null, false, null, null);
    table.io().deleteFile(location);
    // the second writer of the query finds the rewritable deletes in the cache of the daemon
    outputFormat.getHiveRecordWriter(jobConf(table, location, 1), null, null, false, null, null);
  }
}
