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

package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import org.apache.hadoop.fs.LocatedFileStatus;

import org.apache.hadoop.fs.RemoteIterator;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FileMetadataExprType;
import org.apache.hadoop.hive.metastore.api.MetaException;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.HdfsUtils;

public class FileMetadataManager {
  private static final Log LOG = LogFactory.getLog(FileMetadataManager.class);

  private final RawStore tlms;
  private final ExecutorService threadPool;
  private final Configuration conf;

  private final class CacheUpdateRequest implements Callable<Void> {
    FileMetadataExprType type;
    String location;

    public CacheUpdateRequest(FileMetadataExprType type, String location) {
      this.type = type;
      this.location = location;
    }

    @Override
    public Void call() throws Exception {
      try {
        cacheMetadata(type, location);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      } catch (Exception ex) {
        // Nobody can see this exception on the threadpool; just log it.
        LOG.error("Failed to cache file metadata in background for " + type + ", " + location, ex);
      }
      return null;
    }
  }

  public FileMetadataManager(RawStore tlms, Configuration conf) {
    this.tlms = tlms;
    this.conf = conf;
    int numThreads = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.FILE_METADATA_THREADS);
    this.threadPool = Executors.newFixedThreadPool(numThreads,
        new ThreadFactoryBuilder().setNameFormat("File-Metadata-%d").setDaemon(true).build());
  }

  public void queueCacheMetadata(String location, FileMetadataExprType type) {
    threadPool.submit(new CacheUpdateRequest(type, location));
  }

  private void cacheMetadata(FileMetadataExprType type, String location)
      throws MetaException, IOException, InterruptedException {
    Path path = new Path(location);
    FileSystem fs = path.getFileSystem(conf);
    List<Path> files;
    if (!fs.isDirectory(path)) {
      files = Lists.newArrayList(path);
    } else {
      files = new ArrayList<>();
      RemoteIterator<LocatedFileStatus> iter = fs.listFiles(path, true);
      while (iter.hasNext()) {
        // TODO: use fileId right from the list after HDFS-7878; or get dfs client and do it
        LocatedFileStatus lfs = iter.next();
        if (lfs.isDirectory()) continue;
        files.add(lfs.getPath());
      }
    }
    for (Path file : files) {
      long fileId;
      // TODO: use the other HdfsUtils here
      if (!(fs instanceof DistributedFileSystem)) return;
      try {
        fileId = HdfsUtils.getFileId(fs, Path.getPathWithoutSchemeAndAuthority(file).toString());
      } catch (UnsupportedOperationException ex) {
        LOG.error("Cannot cache file metadata for " + location + "; "
            + fs.getClass().getCanonicalName() + " does not support fileId");
        return;
      }
      LOG.info("Caching file metadata for " + file + " (file ID " + fileId + ")");
      file = HdfsUtils.getFileIdPath(fs, file, fileId);
      tlms.getFileMetadataHandler(type).cacheFileMetadata(fileId, fs, file);
    }
  }
}
