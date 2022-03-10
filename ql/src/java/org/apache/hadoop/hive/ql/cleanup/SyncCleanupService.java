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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Dummy cleanup service that just synchronously deletes files.
 * This is created for use in background threads such as compactor
 * or scheduled query runners.
 */
public class SyncCleanupService implements CleanupService {

  private static final Logger LOG = LoggerFactory.getLogger(SyncCleanupService.class.getName());

  public static SyncCleanupService INSTANCE = new SyncCleanupService();

  private SyncCleanupService() {
    //no-op
  }

  @Override
  public void start() {
    //no-op
  }

  @Override
  public boolean deleteRecursive(Path path, FileSystem fileSystem) throws IOException {
    fileSystem.cancelDeleteOnExit(path);
    if (fileSystem.delete(path, true)) {
      LOG.info("Deleted directory: {} on fs with scheme {}", path, fileSystem.getScheme());
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void shutdown() {
    //no-op
  }

  @Override
  public void shutdownNow() {
    //no-op
  }
}
