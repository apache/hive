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
package org.apache.hadoop.hive.ql.io.parquet;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.io.DataCache;
import org.apache.hadoop.hive.common.io.FileMetadataCache;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedInputFormatInterface;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedSupport;
import org.apache.hadoop.hive.ql.io.InputFormatChecker;
import org.apache.hadoop.hive.ql.io.LlapCacheOnlyInputFormatInterface;
import org.apache.hadoop.hive.ql.io.parquet.read.DataWritableReadSupport;
import org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.shrinkStatus;


/**
 * A Parquet InputFormat for Hive (with the deprecated package mapred).
 */
public class MapredParquetInputFormat extends FileInputFormat<NullWritable, ArrayWritable>
  implements InputFormatChecker, VectorizedInputFormatInterface, LlapCacheOnlyInputFormatInterface {

  private static final Logger LOG = LoggerFactory.getLogger(MapredParquetInputFormat.class);

  private final ParquetInputFormat<ArrayWritable> realInput;

  private final transient VectorizedParquetInputFormat vectorizedSelf;

  public MapredParquetInputFormat() {
    this(new ParquetInputFormat<ArrayWritable>(DataWritableReadSupport.class));
  }

  protected MapredParquetInputFormat(final ParquetInputFormat<ArrayWritable> inputFormat) {
    this.realInput = inputFormat;
    vectorizedSelf = new VectorizedParquetInputFormat();
  }

  /**
   * Lists the input paths as {@link FileInputFormat} does, on {@code hive.compute.splits.num.threads} threads, without
   * its glob: on an object store the glob of a partition directory costs a HEAD and a LIST before the directory's own
   * LIST.
   */
  @Override
  protected FileStatus[] listStatus(JobConf job) throws IOException {
    Path[] dirs = getInputPaths(job);
    if (dirs.length == 0) {
      throw new IOException("No input paths specified in job");
    }
    TokenCache.obtainTokensForNamenodes(job.getCredentials(), dirs, job);
    boolean recursive = job.getBoolean(INPUT_DIR_RECURSIVE, false);
    PathFilter jobFilter = getInputPathFilter(job);
    PathFilter filter = path -> FileUtils.HIDDEN_FILES_PATH_FILTER.accept(path)
        && (jobFilter == null || jobFilter.accept(path));

    int numThreads = Math.min(dirs.length, HiveConf.getIntVar(job, HiveConf.ConfVars.HIVE_COMPUTE_SPLITS_NUM_THREADS));
    ExecutorService pool = Executors.newFixedThreadPool(numThreads,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Parquet-ListStatus-%d").build());
    try {
      List<Future<List<FileStatus>>> listings = new ArrayList<>(dirs.length);
      for (Path dir : dirs) {
        listings.add(pool.submit(() -> listInputPath(job, dir, filter, recursive)));
      }
      List<FileStatus> result = new ArrayList<>();
      for (Future<List<FileStatus>> listing : listings) {
        result.addAll(listing.get());
      }
      LOG.info("Total input files to process : {}", result.size());
      return result.toArray(new FileStatus[0]);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new InterruptedIOException("Interrupted while listing the input paths");
    } catch (ExecutionException e) {
      Throwables.throwIfInstanceOf(e.getCause(), IOException.class);
      Throwables.throwIfUnchecked(e.getCause());
      throw new IOException(e.getCause());
    } finally {
      pool.shutdownNow();
    }
  }

  private List<FileStatus> listInputPath(JobConf job, Path dir, PathFilter filter, boolean recursive)
      throws IOException {
    FileSystem fs = dir.getFileSystem(job);
    List<FileStatus> result = new ArrayList<>();
    if (recursive) {
      addInputPathRecursively(result, fs, dir, filter);
    } else {
      RemoteIterator<LocatedFileStatus> statuses = fs.listLocatedStatus(dir);
      while (statuses.hasNext()) {
        LocatedFileStatus status = statuses.next();
        if (filter.accept(status.getPath())) {
          result.add(shrinkStatus(status));
        }
      }
    }
    return result;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public org.apache.hadoop.mapred.RecordReader<NullWritable, ArrayWritable> getRecordReader(
      final org.apache.hadoop.mapred.InputSplit split,
      final org.apache.hadoop.mapred.JobConf job,
      final org.apache.hadoop.mapred.Reporter reporter
      ) throws IOException {
    try {
      if (Utilities.getIsVectorized(job)) {
        LOG.debug("Using vectorized record reader");
        return (RecordReader) vectorizedSelf.getRecordReader(split, job, reporter);
      }
      else {
        LOG.debug("Using row-mode record reader");
        return new ParquetRecordReaderWrapper(realInput, split, job, reporter);
      }
    } catch (final InterruptedException e) {
      throw new RuntimeException("Cannot create a RecordReaderWrapper", e);
    }
  }

  @Override
  public void injectCaches(
      FileMetadataCache metadataCache, DataCache dataCache, Configuration cacheConf) {
    vectorizedSelf.injectCaches(metadataCache, dataCache, cacheConf);
  }

  @Override
  public boolean validateInput(FileSystem fs, HiveConf conf, List<FileStatus> files)
      throws IOException {
    if (files.size() <= 0) return false;

    // The simple validity check is to see if the file is of size 0 or not.
    // Other checks maybe added in the future.
    for (FileStatus file : files) {
      if (file.getLen() == 0) return false;
    }

    return true;
  }

  @Override
  public VectorizedSupport.Support[] getSupportedFeatures() {
    return new VectorizedSupport.Support[] { VectorizedSupport.Support.DECIMAL_64 };
  }
}
