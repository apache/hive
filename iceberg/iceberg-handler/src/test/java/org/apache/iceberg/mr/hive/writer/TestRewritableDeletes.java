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

package org.apache.iceberg.mr.hive.writer;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.ql.exec.ObjectCacheFactory;
import org.apache.hadoop.hive.ql.exec.tez.DagUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DeleteFileSet;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestRewritableDeletes {
  private static final String QUERY_ID = "rewritable_deletes_query";
  private static final String TABLE_NAME = "default.rewritable_deletes";
  private static final Schema SCHEMA = new Schema(optional(1, "id", Types.LongType.get()));

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @After
  public void after() {
    ObjectCacheFactory.removeLlapQueryCache(QUERY_ID);
    LlapProxy.setDaemon(false);
  }

  @Test
  public void testLoadedOncePerDaemon() throws IOException {
    File dir = temp.newFolder();
    Assert.assertTrue(dir.delete());
    Table table = new HadoopTables().create(SCHEMA, PartitionSpec.unpartitioned(),
        ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"), dir.getAbsolutePath());
    DataFile dataFile = DataFiles.builder(table.spec())
        .withPath(dir.getAbsolutePath() + "/data/file.parquet")
        .withFileSizeInBytes(10)
        .withRecordCount(2)
        .build();
    table.newFastAppend().appendFile(dataFile).commit();
    File queryTempDir = temp.newFolder();
    JobConf conf = new JobConf();
    conf.set(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.varname, "tez");
    conf.set(HiveConf.ConfVars.HIVE_QUERY_ID.varname, QUERY_ID);
    conf.set(DagUtils.TEZ_TMP_DIR_KEY, queryTempDir.getAbsolutePath());

    RewritableDeletes.addToJob(table, TABLE_NAME, null, conf);
    Assert.assertArrayEquals("planned without previous deletes", new String[0], queryTempDir.list());

    table.newRowDelta().addDeletes(FileMetadata.deleteFileBuilder(table.spec())
        .ofPositionDeletes()
        .withFormat(FileFormat.PUFFIN)
        .withPath(dir.getAbsolutePath() + "/data/dv.puffin")
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .withReferencedDataFile(dataFile.location())
        .withContentOffset(4)
        .withContentSizeInBytes(6)
        .build()).commit();
    RewritableDeletes.addToJob(table, TABLE_NAME, null, conf);

    LlapProxy.setDaemon(true);
    Map<String, DeleteFileSet> deletes = RewritableDeletes.fromJob(TABLE_NAME, conf);
    Assert.assertEquals(Set.of(dataFile.location()), deletes.keySet());
    // the second writer of the query finds the rewritable deletes in the cache of the daemon
    Assert.assertSame(deletes, RewritableDeletes.fromJob(TABLE_NAME, conf));
  }
}
