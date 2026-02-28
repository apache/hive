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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.hadoop;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

class TestHiveHadoopCommits extends HiveHadoopTableTestBase {

  @Test
  void testCommitFailedBeforeChangeVersionHint() {
    table.newFastAppend().appendFile(FILE_A).commit();
    BaseTable baseTable = (BaseTable) table;
    HadoopTableOperations tableOperations = (HadoopTableOperations) baseTable.operations();

    HadoopTableOperations spyOps2 = (HadoopTableOperations) spy(tableOperations);
    doReturn(10000).when(spyOps2).findVersionWithOutVersionHint(any());
    TableMetadata metadataV1 = spyOps2.current();
    SortOrder dataSort = SortOrder.builderFor(baseTable.schema()).asc("data").build();
    TableMetadata metadataV2 = metadataV1.replaceSortOrder(dataSort);
    assertThatThrownBy(() -> spyOps2.commit(metadataV1, metadataV2))
          .isInstanceOf(CommitFailedException.class)
          .hasMessageContaining("Are there other clients running in parallel with the current task?");

    HadoopTableOperations spyOps3 = (HadoopTableOperations) spy(tableOperations);
    doReturn(false).when(spyOps3).nextVersionIsLatest(anyInt(), anyInt());
    assertCommitNotChangeVersion(
          baseTable,
          spyOps3,
          CommitFailedException.class,
          "Are there other clients running in parallel with the current task?");

    HadoopTableOperations spyOps4 = (HadoopTableOperations) spy(tableOperations);
    doThrow(new RuntimeException("FileSystem crash!"))
          .when(spyOps4)
          .renameMetaDataFileAndCheck(any(), any(), any(), anyBoolean());
    assertCommitNotChangeVersion(
          baseTable, spyOps4, CommitFailedException.class, "FileSystem crash!");
  }

  @Test
  void testCommitFailedAndCheckFailed() throws IOException {
    table.newFastAppend().appendFile(FILE_A).commit();
    BaseTable baseTable = (BaseTable) table;
    HadoopTableOperations tableOperations = (HadoopTableOperations) baseTable.operations();
    HadoopTableOperations spyOps = (HadoopTableOperations) spy(tableOperations);
    doThrow(new IOException("FileSystem crash!"))
          .when(spyOps)
          .renameMetaDataFile(any(), any(), any());
    doThrow(new IOException("Can not check new Metadata!"))
          .when(spyOps)
          .checkMetaDataFileRenameSuccess(any(), any(), any(), anyBoolean());
    assertCommitNotChangeVersion(
          baseTable, spyOps, CommitStateUnknownException.class, "FileSystem crash!");

    HadoopTableOperations spyOps2 = (HadoopTableOperations) spy(tableOperations);
    doThrow(new OutOfMemoryError("Java heap space"))
          .when(spyOps2)
          .renameMetaDataFile(any(), any(), any());
    assertCommitFail(baseTable, spyOps2, OutOfMemoryError.class, "Java heap space");

    HadoopTableOperations spyOps3 = (HadoopTableOperations) spy(tableOperations);
    doThrow(new RuntimeException("UNKNOWN ERROR"))
          .when(spyOps3)
          .renameMetaDataFile(any(), any(), any());
    assertCommitNotChangeVersion(
          baseTable, spyOps3, CommitStateUnknownException.class, "UNKNOWN ERROR");
  }

  @Test
  void testCommitFailedAndRenameNotSuccess() throws IOException {
    table.newFastAppend().appendFile(FILE_A).commit();
    BaseTable baseTable = (BaseTable) table;
    HadoopTableOperations tableOperations = (HadoopTableOperations) baseTable.operations();
    HadoopTableOperations spyOps = (HadoopTableOperations) spy(tableOperations);
    doThrow(new IOException("FileSystem crash!"))
          .when(spyOps)
          .renameMetaDataFile(any(), any(), any());
    assertCommitNotChangeVersion(
          baseTable,
          spyOps,
          CommitFailedException.class,
          "Are there other clients running in parallel with the current task?");
  }

  @Test
  void testCommitFailedButActualSuccess() throws IOException {
    table.newFastAppend().appendFile(FILE_A).commit();
    BaseTable baseTable = (BaseTable) table;
    HadoopTableOperations tableOperations = (HadoopTableOperations) baseTable.operations();
    HadoopTableOperations spyOps = (HadoopTableOperations) spy(tableOperations);
    doThrow(new IOException("FileSystem crash!"))
          .when(spyOps)
          .renameMetaDataFile(any(), any(), any());
    doReturn(true).when(spyOps).checkMetaDataFileRenameSuccess(any(), any(), any(), anyBoolean());
    int versionBefore = spyOps.findVersion();
    TableMetadata metadataV1 = spyOps.current();
    SortOrder dataSort = SortOrder.builderFor(baseTable.schema()).asc("data").build();
    TableMetadata metadataV2 = metadataV1.replaceSortOrder(dataSort);
    spyOps.commit(metadataV1, metadataV2);
    int versionAfter = spyOps.findVersion();
    assertThat(versionAfter).isEqualTo(versionBefore + 1);
  }

  private void assertCommitNotChangeVersion(
        BaseTable baseTable,
        HadoopTableOperations spyOps,
        Class<? extends Throwable> exceptionClass,
        String msg) {
    int versionBefore = spyOps.findVersion();
    assertCommitFail(baseTable, spyOps, exceptionClass, msg);
    int versionAfter = spyOps.findVersion();
    assertThat(versionBefore).isEqualTo(versionAfter);
  }

  private void assertCommitFail(
        BaseTable baseTable,
        HadoopTableOperations spyOps,
        Class<? extends Throwable> exceptionClass,
        String msg) {
    TableMetadata metadataV1 = spyOps.current();
    SortOrder dataSort = SortOrder.builderFor(baseTable.schema()).asc("data").build();
    TableMetadata metadataV2 = metadataV1.replaceSortOrder(dataSort);
    assertThatThrownBy(() -> spyOps.commit(metadataV1, metadataV2))
          .isInstanceOf(exceptionClass)
          .hasMessageContaining(msg);
  }

  @Test
  void testCommitFailedAfterChangeVersionHintRepeatCommit() {
    // Submit a new file to test the functionality.
    table.newFastAppend().appendFile(FILE_A).commit();
    BaseTable baseTable = (BaseTable) table;
    HadoopTableOperations tableOperations = (HadoopTableOperations) baseTable.operations();
    HadoopTableOperations spyOps = (HadoopTableOperations) spy(tableOperations);
    doThrow(new RuntimeException("FileSystem crash!"))
          .when(spyOps)
          .deleteRemovedMetadataFiles(any(), any());
    int versionBefore = spyOps.findVersion();
    TableMetadata metadataV1 = spyOps.current();

    // first commit
    SortOrder dataSort = SortOrder.builderFor(baseTable.schema()).asc("data").build();
    TableMetadata metadataV2 = metadataV1.replaceSortOrder(dataSort);

    spyOps.commit(metadataV1, metadataV2);

    // The commit should actually success
    int versionAfterSecond = spyOps.findVersion();
    assertThat(versionAfterSecond).isEqualTo(versionBefore + 1);

    // second commit
    SortOrder idSort = SortOrder.builderFor(baseTable.schema()).desc("id").build();
    TableMetadata currentMeta = spyOps.current();
    TableMetadata metadataV3 = metadataV2.replaceSortOrder(idSort);
    spyOps.commit(currentMeta, metadataV3);

    // The commit should actually success
    int versionAfterThird = spyOps.findVersion();
    assertThat(versionAfterThird).isEqualTo(versionBefore + 2);
  }

  @Test
  void testTwoClientCommitSameVersion() throws InterruptedException {
    // In the linux environment, the JDK FileSystem interface implementation class is
    // java.io.UnixFileSystem.
    // Its behavior follows the posix protocol, which causes rename operations to overwrite the
    // target file (because linux is compatible with some of the unix interfaces).
    // However, linux also supports renaming without overwriting the target file. In addition, some
    // other file systems such as Windows, HDFS, etc. also support renaming without overwriting the
    // target file.
    // We use the `mv -n` command to simulate the behavior of such filesystems.
    table.newFastAppend().appendFile(FILE_A).commit();
    ExecutorService executorService = Executors.newFixedThreadPool(8);
    AtomicReference<Throwable> unexpectedException = new AtomicReference<>(null);
    AtomicReference<Throwable> expectedException = new AtomicReference<>(null);
    CountDownLatch countDownLatch = new CountDownLatch(2);
    BaseTable baseTable = (BaseTable) table;
    assertThat(((HadoopTableOperations) baseTable.operations()).findVersion()).isEqualTo(2);
    executorService.execute(() -> {
      try {
        HadoopTableOperations tableOperations = (HadoopTableOperations) baseTable.operations();
        HadoopTableOperations spyOps = spy(tableOperations);
        doNothing().when(spyOps).tryLock(any(), any());
        doAnswer(x -> {
          countDownLatch.countDown();
          countDownLatch.await();
          return x.callRealMethod();
        }).when(spyOps).renameMetaDataFileAndCheck(any(), any(), any(), anyBoolean());
        doAnswer(x -> {
          Path srcPath = x.getArgument(1);
          Path dstPath = x.getArgument(2);
          File src = new File(srcPath.toUri());
          File dst = new File(dstPath.toUri());
          String srcPathStr = src.getAbsolutePath();
          String dstPathStr = dst.getAbsolutePath();
          String cmd = String.format("mv -n %s  %s", srcPathStr, dstPathStr);
          Process process = Runtime.getRuntime().exec(cmd);
          assertThat(process.waitFor()).isZero();
          return dst.exists() && !src.exists();
        }).when(spyOps).renameMetaDataFile(any(), any(), any());
        TableMetadata metadataV1 = spyOps.current();
        SortOrder dataSort = SortOrder.builderFor(baseTable.schema()).asc("data").build();
        TableMetadata metadataV2 = metadataV1.replaceSortOrder(dataSort);
        spyOps.commit(metadataV1, metadataV2);
      } catch (CommitFailedException e) {
        expectedException.set(e);
      } catch (Throwable e) {
        unexpectedException.set(e);
      }
    });

    executorService.execute(() -> {
      try {
        HadoopTableOperations tableOperations = (HadoopTableOperations) baseTable.operations();
        HadoopTableOperations spyOps = (HadoopTableOperations) spy(tableOperations);
        doNothing().when(spyOps).tryLock(any(), any());
        doAnswer(x -> {
          countDownLatch.countDown();
          countDownLatch.await();
          return x.callRealMethod();
        }).when(spyOps).renameMetaDataFileAndCheck(any(), any(), any(), anyBoolean());
        doAnswer(x -> {
          Path srcPath = x.getArgument(1);
          Path dstPath = x.getArgument(2);
          File src = new File(srcPath.toUri());
          File dst = new File(dstPath.toUri());
          String srcPathStr = src.getAbsolutePath();
          String dstPathStr = dst.getAbsolutePath();
          String cmd = String.format("mv -n %s  %s", srcPathStr, dstPathStr);
          Process process = Runtime.getRuntime().exec(cmd);
          assertThat(process.waitFor()).isZero();
          return dst.exists() && !src.exists();
        }).when(spyOps).renameMetaDataFile(any(), any(), any());
        TableMetadata metadataV1 = spyOps.current();
        SortOrder dataSort = SortOrder.builderFor(baseTable.schema()).asc("data").build();
        TableMetadata metadataV2 = metadataV1.replaceSortOrder(dataSort);
        spyOps.commit(metadataV1, metadataV2);
      } catch (CommitFailedException e) {
        expectedException.set(e);
      } catch (Throwable e) {
        unexpectedException.set(e);
      }
    });
    executorService.shutdown();
    if (!executorService.awaitTermination(610, TimeUnit.SECONDS)) {
      executorService.shutdownNow();
    }
    assertThat(expectedException.get()).isNotNull();
    assertThat(unexpectedException.get()).isNull();
    assertThatThrownBy(() -> {
      throw expectedException.get();
    }).isInstanceOf(CommitFailedException.class)
          .hasMessageContaining(
                "Can not commit newMetaData because version [3] has already been committed.");
    assertThat(((HadoopTableOperations) baseTable.operations()).findVersion()).isEqualTo(3);
  }

  @Test
  void testConcurrentCommitAndRejectCommitAlreadyExistsVersion()
        throws InterruptedException {
    table.newFastAppend().appendFile(FILE_A).commit();
    ExecutorService executorService = Executors.newFixedThreadPool(8);
    AtomicReference<Throwable> unexpectedException = new AtomicReference<>(null);
    AtomicInteger commitTimes = new AtomicInteger(0);
    int maxCommitTimes = 20;
    CountDownLatch countDownLatch = new CountDownLatch(5);
    CountDownLatch countDownLatch2 = new CountDownLatch(1);
    executorService.execute(() -> {
      try {
        BaseTable baseTable = (BaseTable) table;
        HadoopTableOperations tableOperations = (HadoopTableOperations) baseTable.operations();
        HadoopTableOperations spyOps = (HadoopTableOperations) spy(tableOperations);
        doNothing().when(spyOps).tryLock(any(), any());
        doAnswer(x -> {
          countDownLatch2.countDown();
          countDownLatch.await();
          return x.callRealMethod();
        }).when(spyOps).commitNewVersion(any(), any(), any(), any(), anyBoolean());
        assertCommitFail(
              baseTable, spyOps, CommitFailedException.class, "Version 3 already exists");
      } catch (Throwable e) {
        unexpectedException.set(e);
        throw new RuntimeException(e);
      }
    });

    executorService.execute(() -> {
      try {
        countDownLatch2.await();
        for (; commitTimes.get() < maxCommitTimes; commitTimes.incrementAndGet()) {
          table.newFastAppend().appendFile(FILE_A).commit();
          countDownLatch.countDown();
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    executorService.shutdown();
    if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
      executorService.shutdownNow();
    }
    assertThat(unexpectedException.get()).isNull();
    assertThat(commitTimes.get()).isEqualTo(maxCommitTimes);
  }

  @Test
  void testRejectCommitAlreadyExistsVersionWithUsingObjectStore()
        throws InterruptedException {
    // Since java.io.UnixFileSystem.rename overwrites existing files and we currently share the same
    // memory locks. So we can use the local file system to simulate the use of object storage.
    table.updateProperties().set(TableProperties.OBJECT_STORE_ENABLED, "true").commit();
    table.newFastAppend().appendFile(FILE_A).commit();
    ExecutorService executorService = Executors.newFixedThreadPool(8);
    AtomicReference<Throwable> unexpectedException = new AtomicReference<>(null);
    AtomicInteger commitTimes = new AtomicInteger(0);
    int maxCommitTimes = 20;
    CountDownLatch countDownLatch = new CountDownLatch(5);
    CountDownLatch countDownLatch2 = new CountDownLatch(1);
    executorService.execute(() -> {
      try {
        BaseTable baseTable = (BaseTable) table;
        HadoopTableOperations tableOperations = (HadoopTableOperations) baseTable.operations();
        HadoopTableOperations spyOps = (HadoopTableOperations) spy(tableOperations);
        doAnswer(x -> {
          countDownLatch2.countDown();
          countDownLatch.await();
          return x.callRealMethod();
        }).when(spyOps).tryLock(any(), any());
        assertCommitFail(
              baseTable, spyOps, CommitFailedException.class, "Version 4 already exists");
      } catch (Throwable e) {
        unexpectedException.set(e);
        throw new RuntimeException(e);
      }
    });

    executorService.execute(() -> {
      try {
        countDownLatch2.await();
        for (; commitTimes.get() < maxCommitTimes; commitTimes.incrementAndGet()) {
          table.newFastAppend().appendFile(FILE_A).commit();
          countDownLatch.countDown();
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    executorService.shutdown();
    if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
      executorService.shutdownNow();
    }
    assertThat(unexpectedException.get()).isNull();
    assertThat(commitTimes.get()).isEqualTo(maxCommitTimes);
  }

  @Test
  void testConcurrentCommitAndRejectTooOldCommit() throws InterruptedException {
    // Too-old-commit: commitVersion < currentMaxVersion - METADATA_PREVIOUS_VERSIONS_MAX
    table.newFastAppend().appendFile(FILE_A).commit();
    table.updateProperties().set(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "2").commit();
    table.updateProperties().set(TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, "true")
          .commit();
    ExecutorService executorService = Executors.newFixedThreadPool(8);
    AtomicReference<Throwable> unexpectedException = new AtomicReference<>(null);
    AtomicInteger commitTimes = new AtomicInteger(0);
    int maxCommitTimes = 20;
    CountDownLatch countDownLatch = new CountDownLatch(5);
    CountDownLatch countDownLatch2 = new CountDownLatch(1);
    CountDownLatch countDownLatch3 = new CountDownLatch(1);
    executorService.execute(() -> {
      try {
        BaseTable baseTable = (BaseTable) table;
        HadoopTableOperations tableOperations = (HadoopTableOperations) baseTable.operations();
        HadoopTableOperations spyOps = (HadoopTableOperations) spy(tableOperations);
        doNothing().when(spyOps).tryLock(any(), any());
        doAnswer(x -> {
          countDownLatch2.countDown();
          countDownLatch.await();
          return x.callRealMethod();
        }).when(spyOps).commitNewVersion(any(), any(), any(), any(), anyBoolean());
        assertCommitFail(
              baseTable,
              spyOps,
              CommitFailedException.class,
              "Cannot commit version [5] because it is smaller or " +
                    "much larger than the current latest version [9].");
        countDownLatch3.countDown();
      } catch (Throwable e) {
        unexpectedException.set(e);
        throw new RuntimeException(e);
      }
    });

    executorService.execute(() -> {
      try {
        countDownLatch2.await();
        for (; commitTimes.get() < maxCommitTimes; commitTimes.incrementAndGet()) {
          table.newFastAppend().appendFile(FILE_A).commit();
          countDownLatch.countDown();
          if (countDownLatch.getCount() == 0) {
            countDownLatch3.await();
          }
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    executorService.shutdown();
    if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
      executorService.shutdownNow();
    }
    assertThat(unexpectedException.get()).isNull();
    assertThat(commitTimes.get()).isEqualTo(maxCommitTimes);
  }

  @Test
  void testRejectTooOldCommitWithUsingObjectStore() throws InterruptedException {
    // Since java.io.UnixFileSystem.rename overwrites existing files and we currently share the same
    // memory locks. So we can use the local file system to simulate the use of object storage.
    table.updateProperties().set(TableProperties.OBJECT_STORE_ENABLED, "true").commit();
    table.newFastAppend().appendFile(FILE_A).commit();
    table.updateProperties().set(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "2").commit();
    table.updateProperties()
          .set(TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, "true")
          .commit();
    ExecutorService executorService = Executors.newFixedThreadPool(8);
    AtomicReference<Throwable> unexpectedException = new AtomicReference<>(null);
    AtomicInteger commitTimes = new AtomicInteger(0);
    int maxCommitTimes = 20;
    CountDownLatch countDownLatch = new CountDownLatch(5);
    CountDownLatch countDownLatch2 = new CountDownLatch(1);
    executorService.execute(() -> {
      try {
        BaseTable baseTable = (BaseTable) table;
        HadoopTableOperations tableOperations = (HadoopTableOperations) baseTable.operations();
        HadoopTableOperations spyOps = (HadoopTableOperations) spy(tableOperations);
        doNothing().when(spyOps).tryLock(any(), any());
        doAnswer(x -> {
          countDownLatch2.countDown();
          countDownLatch.await();
          return x.callRealMethod();
        }).when(spyOps).tryLock(any(), any());
        assertCommitFail(
              baseTable,
              spyOps,
              CommitFailedException.class,
              "Cannot commit version [6] because it is smaller or much larger than the current latest version [10].");
      } catch (Throwable e) {
        unexpectedException.set(e);
        throw new RuntimeException(e);
      }
    });

    executorService.execute(() -> {
      try {
        countDownLatch2.await();
        for (; commitTimes.get() < maxCommitTimes; commitTimes.incrementAndGet()) {
          table.newFastAppend().appendFile(FILE_A).commit();
          countDownLatch.countDown();
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    executorService.shutdown();
    if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
      executorService.shutdownNow();
    }
    assertThat(unexpectedException.get()).isNull();
    assertThat(commitTimes.get()).isEqualTo(maxCommitTimes);
  }

  @Test
  void testConcurrentCommitAndRejectDirtyCommit() throws InterruptedException {
    // At the end of the commit, if we realize that it is a dirty commit, we should fail the commit.
    table.newFastAppend().appendFile(FILE_A).commit();
    table.updateProperties().set(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "2").commit();
    table.updateProperties()
          .set(TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, "true")
          .commit();
    ExecutorService executorService = Executors.newFixedThreadPool(8);
    AtomicReference<Throwable> unexpectedException = new AtomicReference<>(null);
    AtomicInteger commitTimes = new AtomicInteger(0);
    CountDownLatch countDownLatch = new CountDownLatch(5);
    CountDownLatch countDownLatch2 = new CountDownLatch(1);
    int maxCommitTimes = 20;
    executorService.execute(() -> {
      try {
        BaseTable baseTable = (BaseTable) table;
        HadoopTableOperations tableOperations = (HadoopTableOperations) baseTable.operations();
        HadoopTableOperations spyOps = (HadoopTableOperations) spy(tableOperations);
        TableMetadata metadataV1 = spyOps.current();
        SortOrder dataSort = SortOrder.builderFor(baseTable.schema()).asc("data").build();
        TableMetadata metadataV2 = metadataV1.replaceSortOrder(dataSort);
        doNothing().when(spyOps).tryLock(any(), any());
        doAnswer(invocation -> {
          countDownLatch.await();
          return invocation.callRealMethod();
        }).when(spyOps).renameMetaDataFileAndCheck(any(), any(), any(), anyBoolean());
        countDownLatch2.countDown();
        assertThatThrownBy(() -> spyOps.commit(metadataV1, metadataV2))
              .isInstanceOf(CommitStateUnknownException.class)
              .hasMessageContaining("Commit rejected by server!" +
                    "The current commit version [5] is much smaller than the latest version [9]");
      } catch (Throwable e) {
        unexpectedException.set(e);
        throw new RuntimeException(e);
      }
    });

    executorService.execute(() -> {
      try {
        countDownLatch2.await();
        for (; commitTimes.get() < maxCommitTimes; commitTimes.incrementAndGet()) {
          table.newFastAppend().appendFile(FILE_A).commit();
          countDownLatch.countDown();
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    executorService.shutdown();
    if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
      executorService.shutdownNow();
    }
    assertThat(commitTimes.get()).isEqualTo(maxCommitTimes);
    assertThat(unexpectedException.get()).isNull();
  }

  @Test
  void testCleanTooOldDirtyCommit() throws InterruptedException {
    // If there are dirty commits in the metadata for some reason, we need to clean them up.
    table.newFastAppend().appendFile(FILE_A).commit();
    table.updateProperties().set(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "2").commit();
    table.updateProperties()
          .set(TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, "true")
          .commit();
    ExecutorService executorService = Executors.newFixedThreadPool(8);
    AtomicReference<Exception> unexpectedException = new AtomicReference<>(null);
    AtomicInteger commitTimes = new AtomicInteger(0);
    int maxCommitTimes = 20;
    BaseTable baseTable = (BaseTable) table;
    HadoopTableOperations tableOperations = (HadoopTableOperations) baseTable.operations();
    HadoopTableOperations spyOps = (HadoopTableOperations) spy(tableOperations);
    CountDownLatch countDownLatch = new CountDownLatch(5);
    CountDownLatch countDownLatch2 = new CountDownLatch(1);
    AtomicReference<File> dirtyCommitFile = new AtomicReference<>(null);
    executorService.execute(() -> {
      try {
        TableMetadata metadataV1 = spyOps.current();
        int currentVersion = spyOps.findVersion();
        int nextVersion = currentVersion + 1;
        SortOrder dataSort = SortOrder.builderFor(baseTable.schema()).asc("data").build();
        TableMetadata metadataV2 = metadataV1.replaceSortOrder(dataSort);
        doNothing().when(spyOps).tryLock(any(), any());
        doAnswer(invocation -> {
          countDownLatch2.countDown();
          countDownLatch.await();
          return invocation.callRealMethod();
        }).when(spyOps).renameMetaDataFileAndCheck(any(), any(), any(), anyBoolean());
        doNothing().when(spyOps).fastFailIfDirtyCommit(anyInt(), anyInt(), any(), any());
        spyOps.commit(metadataV1, metadataV2);
        Path metadataFile = spyOps.getMetadataFile(nextVersion);
        dirtyCommitFile.set(new File(metadataFile.toUri()));
      } catch (Exception e) {
        unexpectedException.set(e);
        throw new RuntimeException(e);
      }
    });

    executorService.execute(() -> {
      try {
        countDownLatch2.await();
        for (; commitTimes.get() < maxCommitTimes; commitTimes.incrementAndGet()) {
          table.newFastAppend().appendFile(FILE_A).commit();
          countDownLatch.countDown();
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    executorService.shutdown();
    if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
      executorService.shutdownNow();
    }

    assertThat(unexpectedException.get()).isNull();
    assertThat(dirtyCommitFile.get()).isNotNull();
    assertThat(dirtyCommitFile.get()).exists();
    long ttl = 30L * 3600 * 24 * 1000;
    assertThat(dirtyCommitFile.get().setLastModified(System.currentTimeMillis() - ttl)).isTrue();
    table.newFastAppend().appendFile(FILE_A).commit();
    assertThat(dirtyCommitFile.get()).doesNotExist();
  }
}
