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

package org.apache.hadoop.hive.ql.cleanup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestCleanupService {

  private static final String TEMP_DIR = TestCleanupService.class.getName() + "-tempdir";
  private CleanupService cleanupService;

  @After
  public void tearDown() throws IOException {
    if (cleanupService != null) {
      cleanupService.shutdownNow();
    }
    Path p = new Path(TEMP_DIR);
    FileSystem fs = p.getFileSystem(new Configuration());
    fs.delete(p, true);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEventualCleanupService_throwsWhenMisconfigured() {
    cleanupService = new EventualCleanupService(10, 4);
  }

  @Test
  public void testEventualCleanupService_deletesManyFiles() throws IOException, InterruptedException {
    testDeleteManyFiles(new EventualCleanupService(4, 1000), 1000);
  }

  @Test
  public void testEventualCleanupService_deletesManyFilesWithQueueSize4() throws IOException, InterruptedException {
    testDeleteManyFiles(new EventualCleanupService(4, 4), 100);
  }

  @Test
  public void testSyncCleanupService_deletesManyFiles() throws IOException, InterruptedException {
    testDeleteManyFiles(SyncCleanupService.INSTANCE, 10);
  }

  @Test
  public void testEventualCleanupService_finishesCleanupBeforeExit() throws IOException, InterruptedException {
    EventualCleanupService cleanupService = new EventualCleanupService(4, 400);
    testDeleteManyFiles(cleanupService, 400, true);
    assertTrue(cleanupService.await(1, TimeUnit.MINUTES));
  }

  private void testDeleteManyFiles(CleanupService cleanupService, int n) throws IOException, InterruptedException {
    testDeleteManyFiles(cleanupService, n, false);
  }

  private void testDeleteManyFiles(CleanupService cleanupService, int n,
                                   boolean shutdownAfterQueueing) throws IOException, InterruptedException {
    this.cleanupService = cleanupService;
    Configuration conf = new Configuration();
    cleanupService.start();

    Collection<Path> files = createManyFiles(n);

    for (Path p: files) {
      cleanupService.deleteRecursive(p, p.getFileSystem(conf));
    }

    if (shutdownAfterQueueing) {
      cleanupService.shutdown();
    }

    assertTrueEventually(() -> {
      try {
        for (Path p : files) {
          FileSystem fs = p.getFileSystem(conf);
          assertNotNull(fs);
          assertFalse(p + " should not exist", fs.exists(p));
        }
      } catch (Exception e) {
        throw new AssertionError(e);
      }
    });
  }

  private Path createFile(String name) throws IOException {
    Path p = new Path(TEMP_DIR, name);
    FileSystem fs = p.getFileSystem(new Configuration());
    fs.create(p);
    return p;
  }

  private Collection<Path> createManyFiles(int n) throws IOException {
    Collection<Path> files = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      files.add(createFile("many_" + i));
    }
    return files;
  }

  private void assertTrueEventually(AssertTask assertTask) throws InterruptedException {
    assertTrueEventually(assertTask, 100000);
  }

  private void assertTrueEventually(AssertTask assertTask, int timeoutMillis) throws InterruptedException {
    long endTime = System.currentTimeMillis() + timeoutMillis;
    AssertionError assertionError = null;

    while (System.currentTimeMillis() < endTime) {
      try {
        assertTask.call();
        return;
      } catch (AssertionError e) {
        assertionError = e;
        sleep(50);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    throw assertionError;
  }

  private static interface AssertTask {
    void call() throws AssertionError;
  }
}
