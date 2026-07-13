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

package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.ql.io.AcidDirectory;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Progressable;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.Map;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/**
 * Regression tests for HIVE-29708: FileSystem.CACHE leak due to missing finally block
 * in three compactor locations - TxnUtils.findUserToRunAs(), MRCompactor.run().
 */
public class TestFileSystemCacheLeakFix {
  private static final String TEST_USER = "testowner";
  private static final int CYCLES = 5;

  @Test
  public void testTxnUtilsNoFileSystemCacheLeakWhenProxyDoAsFails() throws Exception {
    // Verify FileSystem.CACHE does not grow when proxy user also lacks HDFS permission,
    // causing doAs() to throw and closeAllForUGI to be skipped in findUserToRunAs().
    HiveConf conf = buildConf(AccessDeniedFileSystem.class, AccessDeniedFileSystem.SCHEME);
    Table table = buildTable(AccessDeniedFileSystem.SCHEME);
    assertNoCacheGrowth("TxnUtils.findUserToRunAs",
         () -> TxnUtils.findUserToRunAs(table.getSd().getLocation(), table, conf));
  }

  @Test
  public void testTxnUtilsNoFileSystemCacheLeakWhenServerUserSucceeds() throws Exception {
    // Verify FileSystem.CACHE does not grow when server user successfully stats the
    // table location - no proxy UGI is created, so no CACHE entry is added.
    HiveConf conf = buildConf(AccessGrantedFileSystem.class, AccessGrantedFileSystem.SCHEME);
    Table table = buildTable(AccessGrantedFileSystem.SCHEME);
    assertNoCacheGrowth("TxnUtils.findUserToRunAs (success path)", () -> {
      String owner = TxnUtils.findUserToRunAs(table.getSd().getLocation(), table, conf);
      Assert.assertEquals(AccessGrantedFileSystem.FAKE_OWNER, owner);
    });
  }

  @Test
  public void testMRCompactorNoFileSystemCacheLeakWhenRunDoAsFails() throws Exception {
    // Verify FileSystem.CACHE does not grow when the actual MRCompactor.run(CompactorContext)
    // proxy path accesses FileSystem inside doAs() and then throws.
    // The inner run() is spied to force FileSystem.get() before throwing, ensuring the
    // proxy UGI CACHE entry is created and must be cleaned up via closeAllForUGI.
    HiveConf conf = buildConf(AccessDeniedFileSystem.class, AccessDeniedFileSystem.SCHEME);
    MRCompactor compactor = spy(new MRCompactor(mock(IMetaStoreClient.class)));
    doAnswer((InvocationOnMock inv) -> {
      new Path(AccessDeniedFileSystem.SCHEME + "://host/table").getFileSystem(conf);
      throw new IOException("simulated MR failure");
    }).when(compactor).run(
                any(HiveConf.class),
                any(Table.class),
                any(StorageDescriptor.class),
                any(ValidWriteIdList.class),
                any(CompactionInfo.class),
                any(AcidDirectory.class));

    assertNoCacheGrowth("MRCompactor.run", () -> {
      try {
        compactor.run(buildCompactorContext());
      } catch (Exception ignored) {
         // ignore
      }
    });
  }

  private void assertNoCacheGrowth(String ctx, RunnableWithException op) throws Exception {
    try {
      op.run();
    } catch (Exception ignored) {
        // ignore
    }
    int before = getFileSystemCacheSize();
    for (int i = 0; i < TestFileSystemCacheLeakFix.CYCLES; i++) {
      try {
        op.run();
      } catch (Exception ignored) {
          // ignore
      }
    }
    int after = getFileSystemCacheSize();
    Assert.assertEquals(ctx + ": CACHE grew by " + (after - before) + " (expected 0)", before, after);
  }

  private static HiveConf buildConf(Class<? extends FileSystem> fsClass, String scheme) {
    HiveConf conf = new HiveConf();
    conf.set("fs." + scheme + ".impl", fsClass.getName());
    conf.set("fs." + scheme + ".impl.disable.cache", "false");
    return conf;
  }

  private static Table buildTable(String scheme) {
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation(scheme + ":///warehouse/testdb.db/testtable");
    Table table = new Table();
    table.setDbName("testdb");
    table.setTableName("testtable");
    table.setOwner(TestFileSystemCacheLeakFix.TEST_USER);
    table.setSd(sd);
    return table;
  }

  private static CompactorContext buildCompactorContext() {
    HiveConf conf = buildConf(AccessDeniedFileSystem.class, AccessDeniedFileSystem.SCHEME);
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation(AccessDeniedFileSystem.SCHEME + ":///warehouse/testdb.db/testtable");
    Table table = new Table();
    table.setDbName("testdb");
    table.setTableName("testtable");
    table.setOwner(TestFileSystemCacheLeakFix.TEST_USER);
    table.setSd(sd);
    CompactionInfo ci = new CompactionInfo("testdb", "testtable", null, CompactionType.MAJOR);
    ci.runAs = TestFileSystemCacheLeakFix.TEST_USER;
    return new CompactorContext(conf, table, null, sd, new ValidReaderWriteIdList(), ci, mock(AcidDirectory.class));
  }

  static int getFileSystemCacheSize() throws Exception {
    Field cacheField = FileSystem.class.getDeclaredField("CACHE");
    cacheField.setAccessible(true);
    Object cache = cacheField.get(null);
    Field mapField = cache.getClass().getDeclaredField("map");
    mapField.setAccessible(true);
    return ((Map<?, ?>) mapField.get(cache)).size();
  }

  @FunctionalInterface
  interface RunnableWithException {
    void run() throws Exception;
  }

  /* Mock FileSystem that always throws AccessControlException on getFileStatus,
  * simulating an HDFS directory with drwx------ permissions.
  */
  static class AccessDeniedFileSystem extends FileSystem {
    public static final String SCHEME = "testcachefix";

    @Override
     public URI getUri() {
      return URI.create(SCHEME + ":///");
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
      throw new AccessControlException("Permission denied: access=EXECUTE, inode=\"" + f + "\":hdfs:hdfs:drwx------");
    }

    @Override public void initialize(URI uri, Configuration conf) throws IOException {
      // No-op: this mock does not require real FS initialization
    }
    @Override public FSDataInputStream open(Path f, int bufferSize) throws IOException {
      return null;
    }
    @Override public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize,
                                               short replication, long blockSize,
                                               Progressable progress) throws IOException {
      return null;
    }
    @Override public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
      return null;
    }
    @Override public boolean rename(Path src, Path dst) throws IOException {
      return false;
    }
    @Override public boolean delete(Path f, boolean recursive) throws IOException {
      return false;
    }
    @Override public FileStatus[] listStatus(Path f) throws IOException {
      return new FileStatus[0];
    }
    @Override public void setWorkingDirectory(Path newDir) {
      // No-op: working directory is not used by this mock FileSystem
    }
    @Override public Path getWorkingDirectory() {
      return new Path(SCHEME + ":///");
    }
    @Override public boolean mkdirs(Path f, FsPermission permission) throws IOException {
      return false;
    }
  }

  /**
   * Mock FileSystem that always succeeds on getFileStatus, returning a fixed owner.
  */
  static class AccessGrantedFileSystem extends FileSystem {
    public static final String SCHEME = "testsuccess";
    public static final String FAKE_OWNER = "tableowner";

    @Override
    public URI getUri() {
      return URI.create(SCHEME + ":///");
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
      return new FileStatus(0, true, 0, 0, 0, 0, FsPermission.getDirDefault(), FAKE_OWNER, FAKE_OWNER, f);
    }

    @Override public void initialize(URI uri, Configuration conf) throws IOException {
      // No-op: this mock does not require real FS initialization
    }
    @Override public FSDataInputStream open(Path f, int bufferSize) throws IOException {
      return null;
    }
    @Override public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize,
                                               short replication, long blockSize,
                                               Progressable progress) throws IOException {
      return null;
    }
    @Override public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
      return null;
    }
    @Override public boolean rename(Path src, Path dst) throws IOException {
      return false;
    }
    @Override public boolean delete(Path f, boolean recursive) throws IOException {
      return false;
    }
    @Override public FileStatus[] listStatus(Path f) throws IOException {
      return new FileStatus[0];
    }
    @Override public void setWorkingDirectory(Path newDir) {
      // No-op: working directory is not used by this mock FileSystem
    }
    @Override public Path getWorkingDirectory() {
      return new Path(SCHEME + ":///");
    }
    @Override public boolean mkdirs(Path f, FsPermission permission) throws IOException {
      return false;
    }
  }
}
