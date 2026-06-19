/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.llap.daemon.impl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.service.AbstractService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LocalDirCleaner is an LLAP daemon service to clean up old local files. Under normal circumstances,
 * intermediate/local files are cleaned up (typically at end of the DAG), but daemons crash sometimes,
 * and the attached local disk might end up being the same when a new daemon starts (this applies to
 * on-prem as well as cloud scenarios).
 */
public class LocalDirCleaner extends AbstractService {
  private static final Logger LOG = LoggerFactory.getLogger(LocalDirCleaner.class);

  private List<String> localDirs;

  private long cleanupIntervalSec;
  private long fileModifyTimeThresholdSec;

  ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

  public LocalDirCleaner(String[] localDirs, Configuration conf) {
    super("LocalDirCleaner");
    this.localDirs = Arrays.asList(localDirs);
    this.cleanupIntervalSec = getInterval(conf);
    this.fileModifyTimeThresholdSec = getFileModifyTimeThreshold(conf);
    LOG.info("Initialized local dir cleaner: interval: {}s, threshold: {}s", cleanupIntervalSec,
        fileModifyTimeThresholdSec);
  }

  @Override
  public void serviceStart() throws IOException {
    scheduler.scheduleAtFixedRate(this::cleanup, 0, cleanupIntervalSec, TimeUnit.SECONDS);
  }

  @Override
  public void serviceStop() throws IOException {
    // we can shutdown this service now and ignore leftovers, because under normal circumstances,
    // files from the local dirs are cleaned up (so LocalDirCleaner is a best effort utility)
    scheduler.shutdownNow();
  }

  private long getFileModifyTimeThreshold(Configuration conf) {
    return HiveConf.getTimeVar(conf, HiveConf.ConfVars.LLAP_LOCAL_DIR_CLEANER_FILE_MODIFY_TIME_THRESHOLD,
        TimeUnit.SECONDS);
  }

  private long getInterval(Configuration conf) {
    return HiveConf.getTimeVar(conf, HiveConf.ConfVars.LLAP_LOCAL_DIR_CLEANER_CLEANUP_INTERVAL, TimeUnit.SECONDS);
  }

  private void cleanup() {
    Instant deleteBefore = Instant.now().minus(fileModifyTimeThresholdSec, ChronoUnit.SECONDS);

    localDirs.forEach(localDir -> cleanupPath(deleteBefore, Paths.get(localDir)));
  }

  private void cleanupPath(Instant deleteBefore, Path pathLocalDir) {
    LOG.info("Cleaning up files older than {} from {}", deleteBefore, pathLocalDir);

    try (Stream<Path> files = Files.walk(pathLocalDir)) {
      files.filter(f -> {
        try {
          FileTime modified = Files.getLastModifiedTime(f);
          LOG.debug("Checking: {}, modified: {}", f, modified);
          return Files.isRegularFile(f) && modified.toInstant().isBefore(deleteBefore);
        } catch (IOException ex) {
          LOG.warn("IOException caught while checking file for deletion", ex);
          return false;
        }
      }).forEach(f -> {
        try {
          LOG.info("Delete old local file: {}", f);
          Files.delete(f);
        } catch (IOException ex) {
          LOG.warn("Failed to delete file", ex);
        }
      });
    } catch (IOException e) {
      LOG.warn("IOException caught while walking over local files", e);
    }
  }
}
