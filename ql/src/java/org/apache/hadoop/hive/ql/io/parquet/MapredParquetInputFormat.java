/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.parquet;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.BlobStorageUtils;
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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;


/**
 * A Parquet InputFormat for Hive (with the deprecated package mapred).
 */
public class MapredParquetInputFormat extends FileInputFormat<NullWritable, ArrayWritable>
  implements InputFormatChecker, VectorizedInputFormatInterface, LlapCacheOnlyInputFormatInterface {

  private static final Logger LOG = LoggerFactory.getLogger(MapredParquetInputFormat.class);

  private static ExecutorService threadPool;

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
   * On blob storage with multiple recursive input directories, list them in parallel instead of the
   * default serial per-directory listing that dominates split generation. Listed files flow through
   * the inherited {@link FileInputFormat#getSplits} unchanged; all other cases defer to the default.
   */
  @Override
  protected FileStatus[] listStatus(JobConf job) throws IOException {
    Path[] dirs = getInputPaths(job);
    // Only the recursive case (the Tez default) takes the parallel path; non-recursive listing has
    // subtler sub-directory semantics, so defer to the default.
    if (dirs.length <= 1
        || !job.getBoolean(FileInputFormat.INPUT_DIR_RECURSIVE, false)
        || !BlobStorageUtils.isBlobStorageFileSystem(job, dirs[0].getFileSystem(job))) {
      return super.listStatus(job);
    }

    long start = System.currentTimeMillis();
    // List as the caller's end-user, not the pool threads' login user; FileSystem.get is UGI-keyed.
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

    int numThreads = Math.max(2, HiveConf.getIntVar(job, HiveConf.ConfVars.HIVE_COMPUTE_SPLITS_NUM_THREADS));
    ExecutorService pool = getThreadPool(numThreads);
    CompletionService<List<FileStatus>> completionService = new ExecutorCompletionService<>(pool);

    List<Future<List<FileStatus>>> pathFutures = new ArrayList<>(dirs.length);
    List<FileStatus> files = new ArrayList<>();
    try {
      for (Path dir : dirs) {
        pathFutures.add(completionService.submit(() -> listDirectory(job, dir, ugi)));
      }
      for (int resultsLeft = dirs.length; resultsLeft > 0; resultsLeft--) {
        files.addAll(completionService.take().get());
      }
    } catch (InterruptedException e) {
      cancelFutures(pathFutures);
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while listing input directories", e);

    } catch (ExecutionException e) {
      cancelFutures(pathFutures);
      Throwable cause = e.getCause();

      if (cause instanceof InterruptedException) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted while listing input directories", cause);
      }
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException("Failed to list input directories", cause);
    }

    LOG.info("Parquet parallel listStatus: {} files from {} dirs in {} ms ({} threads)",
        files.size(), dirs.length, System.currentTimeMillis() - start, numThreads);
    return files.toArray(new FileStatus[0]);
  }

  private static List<FileStatus> listDirectory(JobConf job, Path dir, UserGroupInformation ugi)
      throws IOException, InterruptedException {
    return ugi.doAs((PrivilegedExceptionAction<List<FileStatus>>) () -> {
      FileSystem dirFs = dir.getFileSystem(job);
      List<FileStatus> dirFiles = new ArrayList<>();
      FileUtils.listStatusRecursively(dirFs, new FileStatus(0, true, 0, 0, 0, dir), dirFiles);
      return dirFiles;
    });
  }

  private static synchronized ExecutorService getThreadPool(int numThreads) {
    if (threadPool == null) {
      threadPool = Executors.newFixedThreadPool(numThreads,
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat("PARQUET_GET_SPLITS #%d")
              .build());
    }
    return threadPool;
  }

  private static <T> void cancelFutures(List<Future<T>> futures) {
    for (Future<T> future : futures) {
      future.cancel(true);
    }
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
    return null;
  }
}
