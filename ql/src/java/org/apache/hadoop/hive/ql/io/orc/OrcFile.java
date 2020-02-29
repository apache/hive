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

package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.LlapDaemonInfo;
import org.apache.hadoop.hive.llap.LlapUtil;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.orc.FileMetadata;
import org.apache.orc.OrcConf;
import org.apache.orc.PhysicalWriter;
import org.apache.orc.MemoryManager;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.MemoryManagerImpl;
import org.apache.orc.impl.OrcTail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Contains factory methods to read or write ORC files.
 */
public final class OrcFile extends org.apache.orc.OrcFile {
  private static final Logger LOG = LoggerFactory.getLogger(OrcFile.class);
  // unused
  protected OrcFile() {}

  /**
   * Create an ORC file reader.
   * @param fs file system
   * @param path file name to read from
   * @return a new ORC file reader.
   * @throws IOException
   */
  public static Reader createReader(FileSystem fs,
                                    Path path) throws IOException {
    ReaderOptions opts = new ReaderOptions(new Configuration());
    opts.filesystem(fs);
    return new ReaderImpl(path, opts);
  }

  public static class ReaderOptions extends org.apache.orc.OrcFile.ReaderOptions {
    public ReaderOptions(Configuration conf) {
      super(conf);
      useUTCTimestamp(true);
      convertToProlepticGregorian(true);
    }

    public ReaderOptions filesystem(FileSystem fs) {
      super.filesystem(fs);
      return this;
    }

    public ReaderOptions maxLength(long val) {
      super.maxLength(val);
      return this;
    }

    public ReaderOptions fileMetadata(FileMetadata metadata) {
      super.fileMetadata(metadata);
      return this;
    }

    public ReaderOptions orcTail(OrcTail orcTail) {
      super.orcTail(orcTail);
      return this;
    }

    public ReaderOptions useUTCTimestamp(boolean value) {
      super.useUTCTimestamp(value);
      return this;
    }

    public ReaderOptions convertToProlepticGregorian(boolean value) {
      super.convertToProlepticGregorian(value);
      return this;
    }
  }

  public static ReaderOptions readerOptions(Configuration conf) {
    return new ReaderOptions(conf);
  }

  public static Reader createReader(Path path,
                                    ReaderOptions options) throws IOException {
    return new ReaderImpl(path, options);
  }

  @VisibleForTesting
  static class LlapAwareMemoryManager extends MemoryManagerImpl {
    private final double maxLoad;
    private final long totalMemoryPool;

    public LlapAwareMemoryManager(Configuration conf) {
      super(conf);
      maxLoad = OrcConf.MEMORY_POOL.getDouble(conf);
      long memPerExecutor = LlapDaemonInfo.INSTANCE.getMemoryPerExecutor();
      totalMemoryPool = (long) (memPerExecutor * maxLoad);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Using LLAP memory manager for orc writer. memPerExecutor: {} maxLoad: {} totalMemPool: {}",
          LlapUtil.humanReadableByteCount(memPerExecutor), maxLoad, LlapUtil.humanReadableByteCount(totalMemoryPool));
      }
    }

    @Override
    public long getTotalMemoryPool() {
      return totalMemoryPool;
    }
  }

  private static ThreadLocal<MemoryManager> threadLocalOrcLlapMemoryManager = null;

  private static synchronized MemoryManager getThreadLocalOrcLlapMemoryManager(final Configuration conf) {
    if (threadLocalOrcLlapMemoryManager == null) {
      threadLocalOrcLlapMemoryManager = ThreadLocal.withInitial(() -> new LlapAwareMemoryManager(conf));
    }
    return threadLocalOrcLlapMemoryManager.get();
  }

  /**
   * Options for creating ORC file writers.
   */
  public static class WriterOptions extends org.apache.orc.OrcFile.WriterOptions {
    private boolean explicitSchema = false;
    private ObjectInspector inspector = null;
    // Setting the default batch size to 1000 makes the memory check at 5000
    // rows work the same as the row by row writer. (If it was the default 1024,
    // the smallest stripe size would be 5120 rows, which changes the output
    // of some of the tests.)
    private int batchSize = 1000;

    WriterOptions(Properties tableProperties, Configuration conf) {
      super(tableProperties, conf);
      useUTCTimestamp(true);
      if (conf.getBoolean(HiveConf.ConfVars.HIVE_ORC_WRITER_LLAP_MEMORY_MANAGER_ENABLED.varname, true) &&
        LlapProxy.isDaemon()) {
        memory(getThreadLocalOrcLlapMemoryManager(conf));
      }
    }

   /**
     * A required option that sets the object inspector for the rows. If
     * setSchema is not called, it also defines the schema.
     */
    public WriterOptions inspector(ObjectInspector value) {
      this.inspector = value;
      if (!explicitSchema) {
        super.setSchema(OrcInputFormat.convertTypeInfo(
            TypeInfoUtils.getTypeInfoFromObjectInspector(value)));
      }
      return this;
    }

    /**
     * Set the schema for the file. This is a required parameter.
     * @param schema the schema for the file.
     * @return this
     */
    public WriterOptions setSchema(TypeDescription schema) {
      if (schema != null) {
        this.explicitSchema = true;
        super.setSchema(schema);
      }
      return this;
    }

    /**
     * Provide the filesystem for the path, if the client has it available.
     * If it is not provided, it will be found from the path.
     */
    public WriterOptions fileSystem(FileSystem value) {
      super.fileSystem(value);
      return this;
    }

    /**
     * Set the stripe size for the file. The writer stores the contents of the
     * stripe in memory until this memory limit is reached and the stripe
     * is flushed to the HDFS file and the next stripe started.
     */
    public WriterOptions stripeSize(long value) {
      super.stripeSize(value);
      return this;
    }

    /**
     * Set the file system block size for the file. For optimal performance,
     * set the block size to be multiple factors of stripe size.
     */
    public WriterOptions blockSize(long value) {
      super.blockSize(value);
      return this;
    }

    /**
     * Set the distance between entries in the row index. The minimum value is
     * 1000 to prevent the index from overwhelming the data. If the stride is
     * set to 0, no indexes will be included in the file.
     */
    public WriterOptions rowIndexStride(int value) {
      super.rowIndexStride(value);
      return this;
    }

    /**
     * The size of the memory buffers used for compressing and storing the
     * stripe in memory.
     */
    public WriterOptions bufferSize(int value) {
      super.bufferSize(value);
      return this;
    }

    /**
     * Sets whether the HDFS blocks are padded to prevent stripes from
     * straddling blocks. Padding improves locality and thus the speed of
     * reading, but costs space.
     */
    public WriterOptions blockPadding(boolean value) {
      super.blockPadding(value);
      return this;
    }

    /**
     * Sets the encoding strategy that is used to encode the data.
     */
    public WriterOptions encodingStrategy(EncodingStrategy strategy) {
      super.encodingStrategy(strategy);
      return this;
    }

    /**
     * Sets the tolerance for block padding as a percentage of stripe size.
     */
    public WriterOptions paddingTolerance(double value) {
      super.paddingTolerance(value);
      return this;
    }

    /**
     * Comma separated values of column names for which bloom filter is to be created.
     */
    public WriterOptions bloomFilterColumns(String columns) {
      super.bloomFilterColumns(columns);
      return this;
    }

    /**
     * Specify the false positive probability for bloom filter.
     * @param fpp - false positive probability
     * @return this
     */
    public WriterOptions bloomFilterFpp(double fpp) {
      super.bloomFilterFpp(fpp);
      return this;
    }

    /**
     * Sets the generic compression that is used to compress the data.
     */
    public WriterOptions compress(CompressionKind value) {
      super.compress(value.getUnderlying());
      return this;
    }

    /**
     * Sets the generic compression that is used to compress the data.
     */
    public WriterOptions compress(org.apache.orc.CompressionKind value) {
      super.compress(value);
      return this;
    }

    /**
     * Sets the version of the file that will be written.
     */
    public WriterOptions version(Version value) {
      super.version(value);
      return this;
    }

    /**
     * Add a listener for when the stripe and file are about to be closed.
     * @param callback the object to be called when the stripe is closed
     * @return this
     */
    public WriterOptions callback(WriterCallback callback) {
      super.callback(callback);
      return this;
    }

    /**
     * A package local option to set the memory manager.
     */
    public WriterOptions memory(MemoryManager value) {
      super.memory(value);
      return this;
    }

    protected WriterOptions batchSize(int maxSize) {
      batchSize = maxSize;
      return this;
    }

    public WriterOptions physicalWriter(PhysicalWriter writer) {
      super.physicalWriter(writer);
      return this;
    }

    public WriterOptions useUTCTimestamp(boolean value) {
      super.useUTCTimestamp(value);
      return this;
    }

    public WriterOptions setProlepticGregorian(boolean value) {
      super.setProlepticGregorian(value);
      return this;
    }

    ObjectInspector getInspector() {
      return inspector;
    }

    int getBatchSize() {
      return batchSize;
    }
  }

  /**
   * Create a set of writer options based on a configuration.
   * @param conf the configuration to use for values
   * @return A WriterOptions object that can be modified
   */
  public static WriterOptions writerOptions(Configuration conf) {
    return new WriterOptions(null, conf);
  }

  /**
   * Create a set of write options based on a set of table properties and
   * configuration.
   * @param tableProperties the properties of the table
   * @param conf the configuration of the query
   * @return a WriterOptions object that can be modified
   */
  public static WriterOptions writerOptions(Properties tableProperties,
                                            Configuration conf) {
    return new WriterOptions(tableProperties, conf);
  }

  /**
   * Create an ORC file writer. This is the public interface for creating
   * writers going forward and new options will only be added to this method.
   * @param path filename to write to
   * @param opts the options
   * @return a new ORC file writer
   * @throws IOException
   */
  public static Writer createWriter(Path path,
                                    WriterOptions opts
                                    ) throws IOException {
    FileSystem fs = opts.getFileSystem() == null ?
      path.getFileSystem(opts.getConfiguration()) : opts.getFileSystem();

    return new WriterImpl(fs, path, opts);
  }

  /**
   * Create an ORC file writer. This method is provided for API backward
   * compatability with Hive 0.11.
   * @param fs file system
   * @param path filename to write to
   * @param inspector the ObjectInspector that inspects the rows
   * @param stripeSize the number of bytes in a stripe
   * @param compress how to compress the file
   * @param bufferSize the number of bytes to compress at once
   * @param rowIndexStride the number of rows between row index entries or
   *                       0 to suppress all indexes
   * @return a new ORC file writer
   * @throws IOException
   */
  public static Writer createWriter(FileSystem fs,
                                    Path path,
                                    Configuration conf,
                                    ObjectInspector inspector,
                                    long stripeSize,
                                    CompressionKind compress,
                                    int bufferSize,
                                    int rowIndexStride) throws IOException {
    return createWriter(path, writerOptions(conf)
        .inspector(inspector)
        .fileSystem(fs)
        .stripeSize(stripeSize)
        .compress(compress)
        .bufferSize(bufferSize)
        .rowIndexStride(rowIndexStride));
  }
}
