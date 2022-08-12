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
package org.apache.hadoop.hive.ql.exec.repl;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.repl.ReplScope;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.repl.util.FileList;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.repl.dump.HiveWrapper;
import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;
import org.apache.hadoop.hive.ql.parse.repl.load.DumpMetaData;
import org.apache.hadoop.hive.ql.parse.repl.metric.ReplicationMetricCollector;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ Utils.class, ReplDumpTask.class})
@PowerMockIgnore({ "javax.management.*" })
public class TestReplDumpTask {

  protected static final Logger LOG = LoggerFactory.getLogger(TestReplDumpTask.class);

  private static final String HS2_CREDSTORE_PATH =
      Constants.HIVE_REPL_CLOUD_KEYSTORE_TYPE + "://file/path/to/hiveserver2/creds.localjceks";
  private static final String HDFS_CREDSTORE_PATH =
      Constants.HIVE_REPL_CLOUD_KEYSTORE_TYPE + "://hdfs@source.com:8020/hive-replication/creds.jceks";

  @Mock
  private Hive hive;

  @Mock
  private HiveConf conf;

  @Mock
  private QueryState queryState;

  @Mock
  ReplicationMetricCollector metricCollector;

  class StubReplDumpTask extends ReplDumpTask {

    @Override
    protected Hive getHive() {
      return hive;
    }

    @Override
    long currentNotificationId(Hive hiveDb) {
      return Long.MAX_VALUE;
    }

    @Override
    String getValidTxnListForReplDump(Hive hiveDb, long waitUntilTime) {
      return "";
    }

    @Override
    List<EximUtil.DataCopyPath> dumpFunctionMetadata(String dbName, Path dbMetadataRoot, Path dbDataRoot,
                                                     Hive hiveDb, boolean copyAtLoad) {
      return Collections.emptyList();
    }

    @Override
    Path dumpDbMetadata(String dbName, Path metadataRoot, long lastReplId, Hive hiveDb) {
      return Mockito.mock(Path.class);
    }

    @Override
    void dumpConstraintMetadata(String dbName, String tblName, Path dbRoot, Hive hiveDb, long tableId) {
    }
  }

  private static class TestException extends Exception {
  }

  @Test(expected = TestException.class)
  public void removeDBPropertyToPreventRenameWhenBootstrapDumpOfTableFails() throws Exception {
    List<String> tableList = Arrays.asList("a1", "a2");
    String dbRandomKey = "akeytoberandom";
    ReplScope replScope = new ReplScope("default");

    mockStatic(Utils.class);
    when(Utils.matchesDb(same(hive), eq("default")))
        .thenReturn(Collections.singletonList("default"));
    when(Utils.getAllTables(same(hive), eq("default"), eq(replScope))).thenReturn(tableList);
    when(Utils.setDbBootstrapDumpState(same(hive), eq("default"))).thenReturn(dbRandomKey);
    when(Utils.matchesTbl(same(hive), eq("default"), eq(replScope))).thenReturn(tableList);


    when(hive.getAllFunctions()).thenReturn(Collections.emptyList());
    when(queryState.getConf()).thenReturn(conf);
    when(conf.getLong("hive.repl.last.repl.id", -1L)).thenReturn(1L);
    when(conf.getBoolVar(HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES)).thenReturn(false);
    when(HiveConf.getVar(conf, HiveConf.ConfVars.REPL_BOOTSTRAP_DUMP_OPEN_TXN_TIMEOUT)).thenReturn("1h");
    whenNew(HiveWrapper.class).withAnyArguments().thenReturn(mock(HiveWrapper.class));

    ReplDumpTask task = new StubReplDumpTask() {
      private int tableDumpCount = 0;

      @Override
      void dumpTable(String dbName, String tblName, String validTxnList,
                     Path dbRootMetadata, Path dbRootData,
                     long lastReplId, Hive hiveDb,
                     HiveWrapper.Tuple<Table> tuple, FileList managedTableDirFileList, boolean dataCopyAtLoad)
          throws Exception {
        tableDumpCount++;
        if (tableDumpCount > 1) {
          throw new TestException();
        }
      }
    };

    task.initialize(queryState, null, null, null);
    ReplDumpWork replDumpWork = new ReplDumpWork(replScope, "", "");
    replDumpWork.setMetricCollector(metricCollector);
    task.setWork(replDumpWork);

    try {
      task.bootStrapDump(new Path("mock"), new DumpMetaData(new Path("mock"), conf),
        mock(Path.class), hive);
    } finally {
      Utils.resetDbBootstrapDumpState(same(hive), eq("default"), eq(dbRandomKey));
    }
  }

  @Test
  public void testHS2KeystoreSchemeNotUpdatedIfNotCloud() throws IOException {
    when(conf.get(Constants.HIVE_REPL_CLOUD_CREDENTIAL_PROVIDER_PATH)).thenReturn(null);
    when(conf.get(Constants.HADOOP_CREDENTIAL_PROVIDER_PATH_CONFIG)).thenReturn(HS2_CREDSTORE_PATH);

    ReplDumpTask replDumpTask = createReplDumpTask(conf);
    replDumpTask.execute();

    verify(conf, never()).set(any(), any());
  }

  @Test
  public void testHS2KeystoreSchemeUpdatedIfCloud() throws IOException {
    when(conf.get(Constants.HIVE_REPL_CLOUD_CREDENTIAL_PROVIDER_PATH)).thenReturn(HDFS_CREDSTORE_PATH);
    when(conf.get(Constants.HADOOP_CREDENTIAL_PROVIDER_PATH_CONFIG)).thenReturn(HS2_CREDSTORE_PATH);

    ReplDumpTask replDumpTask = createReplDumpTask(conf);
    replDumpTask.execute();

    String updatedHs2CredstorePath = HS2_CREDSTORE_PATH.replaceFirst(
        Constants.HIVE_REPL_CLOUD_KEYSTORE_TYPE, Constants.HIVE_REPL_CLOUD_SCHEME_NAME
    );
    verify(conf).set(Constants.HADOOP_CREDENTIAL_PROVIDER_PATH_CONFIG, updatedHs2CredstorePath);
    verify(conf, never()).set(eq(Constants.HIVE_REPL_CLOUD_CREDENTIAL_PROVIDER_PATH), any());
  }

  private ReplDumpTask createReplDumpTask(HiveConf conf) throws IOException {
    ReplDumpWork work = mock(ReplDumpWork.class);
    when(work.dataCopyIteratorsInitialized()).thenReturn(true);
    List<String> externalTblCopyPaths = Collections.singletonList("/path/to/external/tbl/copy");
    when(work.getExternalTblCopyPathIterator()).thenReturn(externalTblCopyPaths.iterator());
    Task<?> task = mock(Task.class);
    List<Task<?>> tasks = Collections.singletonList(task);
    when(work.externalTableCopyTasks(any(), any())).thenReturn(tasks);
    return new ReplDumpTask(conf, work);
  }

}
