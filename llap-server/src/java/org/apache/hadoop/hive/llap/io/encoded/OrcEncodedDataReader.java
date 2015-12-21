/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.llap.io.encoded;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.Pool;
import org.apache.hadoop.hive.common.Pool.PoolObjectHelper;
import org.apache.hadoop.hive.common.io.DataCache;
import org.apache.hadoop.hive.common.io.Allocator;
import org.apache.hadoop.hive.common.io.encoded.EncodedColumnBatch.ColumnStreamData;
import org.apache.hadoop.hive.common.io.DiskRange;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.ConsumerFeedback;
import org.apache.hadoop.hive.llap.DebugUtils;
import org.apache.hadoop.hive.llap.cache.Cache;
import org.apache.hadoop.hive.llap.cache.LowLevelCache;
import org.apache.hadoop.hive.llap.cache.LowLevelCache.Priority;
import org.apache.hadoop.hive.llap.counters.QueryFragmentCounters;
import org.apache.hadoop.hive.llap.counters.QueryFragmentCounters.Counter;
import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;
import org.apache.hadoop.hive.llap.io.decode.OrcEncodedDataConsumer;
import org.apache.hadoop.hive.llap.io.metadata.OrcFileMetadata;
import org.apache.hadoop.hive.llap.io.metadata.OrcMetadataCache;
import org.apache.hadoop.hive.llap.io.metadata.OrcStripeMetadata;
import org.apache.hadoop.hive.ql.exec.DDLTask;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.HdfsUtils;
import org.apache.orc.CompressionKind;
import org.apache.orc.DataReader;
import org.apache.orc.impl.MetadataReader;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.ReaderOptions;
import org.apache.orc.OrcConf;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSplit;
import org.apache.hadoop.hive.ql.io.orc.encoded.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl.SargApplier;
import org.apache.hadoop.hive.ql.io.orc.encoded.Consumer;
import org.apache.hadoop.hive.ql.io.orc.encoded.EncodedOrcFile;
import org.apache.hadoop.hive.ql.io.orc.encoded.EncodedReader;
import org.apache.hadoop.hive.ql.io.orc.encoded.OrcBatchKey;
import org.apache.hadoop.hive.ql.io.orc.encoded.OrcCacheKey;
import org.apache.hadoop.hive.ql.io.orc.encoded.Reader.OrcEncodedColumnBatch;
import org.apache.hadoop.hive.ql.io.orc.encoded.Reader.PoolFactory;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderUtils;
import org.apache.orc.StripeInformation;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.common.util.FixedSizedObjectPool;
import org.apache.orc.OrcProto;
import org.apache.tez.common.CallableWithNdc;

/**
 * This produces EncodedColumnBatch via ORC EncodedDataImpl.
 * It serves as Consumer for EncodedColumnBatch too, for the high-level cache scenario where
 * it inserts itself into the pipeline to put the data in cache, before passing it to the real
 * consumer. It also serves as ConsumerFeedback that receives processed EncodedColumnBatch-es.
 */
public class OrcEncodedDataReader extends CallableWithNdc<Void>
    implements ConsumerFeedback<OrcEncodedColumnBatch>, Consumer<OrcEncodedColumnBatch> {
  private static final Logger LOG = LoggerFactory.getLogger(OrcEncodedDataReader.class);
  public static final FixedSizedObjectPool<ColumnStreamData> CSD_POOL =
      new FixedSizedObjectPool<>(8192, new PoolObjectHelper<ColumnStreamData>() {
        @Override
        public ColumnStreamData create() {
          return new ColumnStreamData();
        }
        @Override
        public void resetBeforeOffer(ColumnStreamData t) {
          t.reset();
        }
      });
  public static final FixedSizedObjectPool<OrcEncodedColumnBatch> ECB_POOL =
      new FixedSizedObjectPool<>(1024, new PoolObjectHelper<OrcEncodedColumnBatch>() {
        @Override
        public OrcEncodedColumnBatch create() {
          return new OrcEncodedColumnBatch();
        }
        @Override
        public void resetBeforeOffer(OrcEncodedColumnBatch t) {
          t.reset();
        }
      });
  private static final PoolFactory POOL_FACTORY = new PoolFactory() {
    @Override
    public <T> Pool<T> createPool(int size, PoolObjectHelper<T> helper) {
      return new FixedSizedObjectPool<>(size, helper);
    }

    @Override
    public Pool<ColumnStreamData> createColumnStreamDataPool() {
      return CSD_POOL;
    }

    @Override
    public Pool<OrcEncodedColumnBatch> createEncodedColumnBatchPool() {
      return ECB_POOL;
    }
  };

  private final OrcMetadataCache metadataCache;
  private final LowLevelCache lowLevelCache;
  private final Configuration conf;
  private final Cache<OrcCacheKey> cache;
  private final FileSplit split;
  private List<Integer> columnIds;
  private final SearchArgument sarg;
  private final String[] columnNames;
  private final OrcEncodedDataConsumer consumer;
  private final QueryFragmentCounters counters;
  private final UserGroupInformation ugi;

  // Read state.
  private int stripeIxFrom;
  private OrcFileMetadata fileMetadata;
  private Reader orcReader;
  private MetadataReader metadataReader;
  private EncodedReader stripeReader;
  private Long fileId;
  private FileSystem fs;
  /**
   * readState[stripeIx'][colIx'] => boolean array (could be a bitmask) of rg-s that need to be
   * read. Contains only stripes that are read, and only columns included. null => read all RGs.
   */
  private boolean[][][] readState;
  private volatile boolean isStopped = false;
  @SuppressWarnings("unused")
  private volatile boolean isPaused = false;

  public OrcEncodedDataReader(LowLevelCache lowLevelCache, Cache<OrcCacheKey> cache,
      OrcMetadataCache metadataCache, Configuration conf, FileSplit split,
      List<Integer> columnIds, SearchArgument sarg, String[] columnNames,
      OrcEncodedDataConsumer consumer, QueryFragmentCounters counters) {
    this.lowLevelCache = lowLevelCache;
    this.metadataCache = metadataCache;
    this.cache = cache;
    this.conf = conf;
    this.split = split;
    this.columnIds = columnIds;
    if (this.columnIds != null) {
      Collections.sort(this.columnIds);
    }
    this.sarg = sarg;
    this.columnNames = columnNames;
    this.consumer = consumer;
    this.counters = counters;
    try {
      this.ugi = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void stop() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Encoded reader is being stopped");
    }
    isStopped = true;
  }

  @Override
  public void pause() {
    isPaused = true;
    // TODO: pause fetching
  }

  @Override
  public void unpause() {
    isPaused = false;
    // TODO: unpause fetching
  }

  @Override
  protected Void callInternal() throws IOException, InterruptedException {
    return ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        return performDataRead();
      }
    });
  }

  protected Void performDataRead() throws IOException {
    long startTime = counters.startTimeCounter();
    if (LlapIoImpl.LOGL.isInfoEnabled()) {
      LlapIoImpl.LOG.info("Processing data for " + split.getPath());
    }
    if (processStop()) {
      recordReaderTime(startTime);
      return null;
    }
    counters.setDesc(QueryFragmentCounters.Desc.TABLE, getDbAndTableName(split.getPath()));
    orcReader = null;
    // 1. Get file metadata from cache, or create the reader and read it.
    // Don't cache the filesystem object for now; Tez closes it and FS cache will fix all that
    fs = split.getPath().getFileSystem(conf);
    fileId = determineFileId(fs, split,
        HiveConf.getBoolVar(conf, ConfVars.LLAP_CACHE_ALLOW_SYNTHETIC_FILEID));
    counters.setDesc(QueryFragmentCounters.Desc.FILE, split.getPath()
        + (fileId == null ? "" : " (" + fileId + ")"));

    try {
      fileMetadata = getOrReadFileMetadata();
      consumer.setFileMetadata(fileMetadata);
      validateFileMetadata();
      if (columnIds == null) {
        columnIds = createColumnIds(fileMetadata);
      }

      // 2. Determine which stripes to read based on the split.
      determineStripesToRead();
    } catch (Throwable t) {
      recordReaderTime(startTime);
      consumer.setError(t);
      return null;
    }

    if (readState.length == 0) {
      consumer.setDone();
      recordReaderTime(startTime);
      return null; // No data to read.
    }
    counters.setDesc(QueryFragmentCounters.Desc.STRIPES, stripeIxFrom + "," + readState.length);

    // 3. Apply SARG if needed, and otherwise determine what RGs to read.
    int stride = fileMetadata.getRowIndexStride();
    ArrayList<OrcStripeMetadata> stripeMetadatas = null;
    boolean[] globalIncludes = null;
    boolean[] sargColumns = null;
    try {
      globalIncludes = OrcInputFormat.genIncludedColumns(fileMetadata.getTypes(), columnIds, true);
      if (sarg != null && stride != 0) {
        // TODO: move this to a common method
        int[] filterColumns = RecordReaderImpl.mapSargColumnsToOrcInternalColIdx(
          sarg.getLeaves(), columnNames, 0);
        // included will not be null, row options will fill the array with trues if null
        sargColumns = new boolean[globalIncludes.length];
        for (int i : filterColumns) {
          // filter columns may have -1 as index which could be partition column in SARG.
          if (i > 0) {
            sargColumns[i] = true;
          }
        }

        // If SARG is present, get relevant stripe metadata from cache or readers.
        stripeMetadatas = readStripesMetadata(globalIncludes, sargColumns);
      }

      // Now, apply SARG if any; w/o sarg, this will just initialize readState.
      boolean hasData = determineRgsToRead(globalIncludes, stride, stripeMetadatas);
      if (!hasData) {
        consumer.setDone();
        recordReaderTime(startTime);
        return null; // No data to read.
      }
    } catch (Throwable t) {
      cleanupReaders();
      consumer.setError(t);
      recordReaderTime(startTime);
      return null;
    }

    if (processStop()) {
      cleanupReaders();
      recordReaderTime(startTime);
      return null;
    }

    // 4. Get data from high-level cache.
    //    If some cols are fully in cache, this will also give us the modified list of columns to
    //    read for every stripe (null means read all of them - the usual path). In any case,
    //    readState will be modified for column x rgs that were fetched from high-level cache.
    List<Integer>[] stripeColsToRead = null;
    if (cache != null) {
      try {
        stripeColsToRead = produceDataFromCache(stride);
      } catch (Throwable t) {
        // produceDataFromCache handles its own cleanup.
        consumer.setError(t);
        cleanupReaders();
        recordReaderTime(startTime);
        return null;
      }
    }

    // 5. Create encoded data reader.
    // In case if we have high-level cache, we will intercept the data and add it there;
    // otherwise just pass the data directly to the consumer.
    Consumer<OrcEncodedColumnBatch> dataConsumer = (cache == null) ? this.consumer : this;
    try {
      ensureOrcReader();
      // Reader creating updates HDFS counters, don't do it here.
      DataWrapperForOrc dw = new DataWrapperForOrc();
      stripeReader = orcReader.encodedReader(fileId, dw, dw, POOL_FACTORY);
      stripeReader.setDebugTracing(DebugUtils.isTraceOrcEnabled());
    } catch (Throwable t) {
      consumer.setError(t);
      recordReaderTime(startTime);
      cleanupReaders();
      return null;
    }

    // 6. Read data.
    // TODO: I/O threadpool could be here - one thread per stripe; for now, linear.
    boolean hasFileId = this.fileId != null;
    long fileId = hasFileId ? this.fileId : 0;
    OrcBatchKey stripeKey = hasFileId ? new OrcBatchKey(fileId, -1, 0) : null;
    for (int stripeIxMod = 0; stripeIxMod < readState.length; ++stripeIxMod) {
      if (processStop()) {
        cleanupReaders();
        recordReaderTime(startTime);
        return null;
      }
      int stripeIx = stripeIxFrom + stripeIxMod;
      boolean[][] colRgs = null;
      boolean[] stripeIncludes = null;
      OrcStripeMetadata stripeMetadata = null;
      StripeInformation stripe;
      try {
        List<Integer> cols = stripeColsToRead == null ? null : stripeColsToRead[stripeIxMod];
        if (cols != null && cols.isEmpty()) continue; // No need to read this stripe.
        stripe = fileMetadata.getStripes().get(stripeIx);

        if (DebugUtils.isTraceOrcEnabled()) {
          LlapIoImpl.LOG.info("Reading stripe " + stripeIx + ": "
              + stripe.getOffset() + ", " + stripe.getLength());
        }
        colRgs = readState[stripeIxMod];
        // We assume that NO_RGS value is only set from SARG filter and for all columns;
        // intermediate changes for individual columns will unset values in the array.
        // Skip this case for 0-column read. We could probably special-case it just like we do
        // in EncodedReaderImpl, but for now it's not that important.
        if (colRgs.length > 0 && colRgs[0] == SargApplier.READ_NO_RGS) continue;

        // 6.1. Determine the columns to read (usually the same as requested).
        if (cache == null || cols == null || cols.size() == colRgs.length) {
          cols = columnIds;
          stripeIncludes = globalIncludes;
        } else {
          // We are reading subset of the original columns, remove unnecessary bitmasks/etc.
          // This will never happen w/o high-level cache.
          stripeIncludes = OrcInputFormat.genIncludedColumns(fileMetadata.getTypes(), cols, true);
          colRgs = genStripeColRgs(cols, colRgs);
        }

        // 6.2. Ensure we have stripe metadata. We might have read it before for RG filtering.
        boolean isFoundInCache = false;
        if (stripeMetadatas != null) {
          stripeMetadata = stripeMetadatas.get(stripeIxMod);
        } else {
          if (hasFileId) {
            stripeKey.stripeIx = stripeIx;
            stripeMetadata = metadataCache.getStripeMetadata(stripeKey);
          }
          isFoundInCache = (stripeMetadata != null);
          if (!isFoundInCache) {
            counters.incrCounter(Counter.METADATA_CACHE_MISS);
            ensureMetadataReader();
            long startTimeHdfs = counters.startTimeCounter();
            stripeMetadata = new OrcStripeMetadata(
                stripeKey, metadataReader, stripe, stripeIncludes, sargColumns);
            counters.incrTimeCounter(Counter.HDFS_TIME_US, startTimeHdfs);
            if (hasFileId) {
              stripeMetadata = metadataCache.putStripeMetadata(stripeMetadata);
              if (DebugUtils.isTraceOrcEnabled()) {
                LlapIoImpl.LOG.info("Caching stripe " + stripeKey.stripeIx
                    + " metadata with includes: " + DebugUtils.toString(stripeIncludes));
              }
              stripeKey = new OrcBatchKey(fileId, -1, 0);
            }

          }
          consumer.setStripeMetadata(stripeMetadata);
        }
        if (!stripeMetadata.hasAllIndexes(stripeIncludes)) {
          if (DebugUtils.isTraceOrcEnabled()) {
            LlapIoImpl.LOG.info("Updating indexes in stripe " + stripeKey.stripeIx
                + " metadata for includes: " + DebugUtils.toString(stripeIncludes));
          }
          assert isFoundInCache;
          counters.incrCounter(Counter.METADATA_CACHE_MISS);
          ensureMetadataReader();
          updateLoadedIndexes(stripeMetadata, stripe, stripeIncludes, sargColumns);
        } else if (isFoundInCache) {
          counters.incrCounter(Counter.METADATA_CACHE_HIT);
        }
      } catch (Throwable t) {
        consumer.setError(t);
        cleanupReaders();
        recordReaderTime(startTime);
        return null;
      }
      if (processStop()) {
        cleanupReaders();
        recordReaderTime(startTime);
        return null;
      }

      // 6.3. Finally, hand off to the stripe reader to produce the data.
      //      This is a sync call that will feed data to the consumer.
      try {
        // TODO: readEncodedColumns is not supposed to throw; errors should be propagated thru
        // consumer. It is potentially holding locked buffers, and must perform its own cleanup.
        // Also, currently readEncodedColumns is not stoppable. The consumer will discard the
        // data it receives for one stripe. We could probably interrupt it, if it checked that.
        stripeReader.readEncodedColumns(stripeIx, stripe, stripeMetadata.getRowIndexes(),
            stripeMetadata.getEncodings(), stripeMetadata.getStreams(), stripeIncludes,
            colRgs, dataConsumer);
      } catch (Throwable t) {
        consumer.setError(t);
        cleanupReaders();
        recordReaderTime(startTime);
        return null;
      }
    }

    // Done with all the things.
    recordReaderTime(startTime);
    dataConsumer.setDone();
    if (DebugUtils.isTraceMttEnabled()) {
      LlapIoImpl.LOG.info("done processing " + split);
    }

    // Close the stripe reader, we are done reading.
    cleanupReaders();
    return null;
  }

  private void recordReaderTime(long startTime) {
    counters.incrTimeCounter(Counter.TOTAL_IO_TIME_US, startTime);
  }

  private static String getDbAndTableName(Path path) {
    // Ideally, we'd get this from split; however, split doesn't contain any such thing and it's
    // actually pretty hard to get cause even split generator only uses paths. We only need this
    // for metrics; therefore, brace for BLACK MAGIC!
    String[] parts = path.toUri().getPath().toString().split(Path.SEPARATOR);
    int dbIx = -1;
    // Try to find the default db postfix; don't check two last components - at least there
    // should be a table and file (we could also try to throw away partition/bucket/acid stuff).
    for (int i = 0; i < parts.length - 2; ++i) {
      if (!parts[i].endsWith(DDLTask.DATABASE_PATH_SUFFIX)) continue;
      if (dbIx >= 0) {
        dbIx = -1; // Let's not guess.
        break;
      }
      dbIx = i;
    }
    if (dbIx >= 0) {
      return parts[dbIx].substring(0, parts[dbIx].length() - 3) + "." + parts[dbIx + 1];
    }

    // Just go from the back and throw away everything we think is wrong; skip last item, the file.
    boolean isInPartFields = false;
    for (int i = parts.length - 2; i >= 0; --i) {
      String p = parts[i];
      boolean isPartField = p.contains("=");
      if ((isInPartFields && !isPartField) || (!isPartField && !p.startsWith(AcidUtils.BASE_PREFIX)
          && !p.startsWith(AcidUtils.DELTA_PREFIX) && !p.startsWith(AcidUtils.BUCKET_PREFIX))) {
        dbIx = i - 1;
        break;
      }
      isInPartFields = isPartField;
    }
    // If we found something before we ran out of components, use it.
    if (dbIx >= 0) {
      String dbName = parts[dbIx];
      if (dbName.endsWith(DDLTask.DATABASE_PATH_SUFFIX)) {
        dbName = dbName.substring(0, dbName.length() - 3);
      }
      return dbName + "." + parts[dbIx + 1];
    }
    return "unknown";
  }

  private void validateFileMetadata() throws IOException {
    if (fileMetadata.getCompressionKind() == CompressionKind.NONE) return;
    int bufferSize = fileMetadata.getCompressionBufferSize();
    int minAllocSize = HiveConf.getIntVar(conf, HiveConf.ConfVars.LLAP_ORC_CACHE_MIN_ALLOC);
    if (bufferSize < minAllocSize) {
      LOG.warn("ORC compression buffer size (" + bufferSize + ") is smaller than LLAP low-level "
            + "cache minimum allocation size (" + minAllocSize + "). Decrease the value for "
            + HiveConf.ConfVars.LLAP_ORC_CACHE_MIN_ALLOC.toString() + " to avoid wasting memory");
    }
  }

  private boolean processStop() {
    if (!isStopped) return false;
    LOG.info("Encoded data reader is stopping");
    cleanupReaders();
    return true;
  }

  private static Long determineFileId(FileSystem fs, FileSplit split,
      boolean allowSynthetic) throws IOException {
    if (split instanceof OrcSplit) {
      Long fileId = ((OrcSplit)split).getFileId();
      if (fileId != null) {
        return fileId;
      }
    }
    LOG.warn("Split for " + split.getPath() + " (" + split.getClass() + ") does not have file ID");
    return HdfsUtils.getFileId(fs, split.getPath(), allowSynthetic);
  }

  private boolean[][] genStripeColRgs(List<Integer> stripeCols, boolean[][] globalColRgs) {
    boolean[][] stripeColRgs = new boolean[stripeCols.size()][];
    for (int i = 0, i2 = -1; i < globalColRgs.length; ++i) {
      if (globalColRgs[i] == null) continue;
      stripeColRgs[i2] = globalColRgs[i];
      ++i2;
    }
    return stripeColRgs;
  }

  /**
   * Puts all column indexes from metadata to make a column list to read all column.
   */
  private static List<Integer> createColumnIds(OrcFileMetadata metadata) {
    List<Integer> columnIds = new ArrayList<Integer>(metadata.getTypes().size());
    for (int i = 1; i < metadata.getTypes().size(); ++i) {
      columnIds.add(i);
    }
    return columnIds;
  }

  /**
   * In case if stripe metadata in cache does not have all indexes for current query, load
   * the missing one. This is a temporary cludge until real metadata cache becomes available.
   */
  private void updateLoadedIndexes(OrcStripeMetadata stripeMetadata,
      StripeInformation stripe, boolean[] stripeIncludes, boolean[] sargColumns) throws IOException {
    // We only synchronize on write for now - design of metadata cache is very temporary;
    // we pre-allocate the array and never remove entries; so readers should be safe.
    synchronized (stripeMetadata) {
      if (stripeMetadata.hasAllIndexes(stripeIncludes)) return;
      long startTime = counters.startTimeCounter();
      stripeMetadata.loadMissingIndexes(metadataReader, stripe, stripeIncludes, sargColumns);
      counters.incrTimeCounter(Counter.HDFS_TIME_US, startTime);
    }
  }

  /**
   * Closes the stripe readers (on error).
   */
  private void cleanupReaders() {
    if (metadataReader != null) {
      try {
        metadataReader.close();
      } catch (IOException ex) {
        // Ignore.
      }
    }
    if (stripeReader != null) {
      try {
        stripeReader.close();
      } catch (IOException ex) {
        // Ignore.
      }
    }
  }

  /**
   * Ensures orcReader is initialized for the split.
   */
  private void ensureOrcReader() throws IOException {
    if (orcReader != null) return;
    Path path = split.getPath();
    if (fileId != null && HiveConf.getBoolVar(conf, ConfVars.LLAP_IO_USE_FILEID_PATH)) {
      path = HdfsUtils.getFileIdPath(fs, path, fileId);
    }
    if (DebugUtils.isTraceOrcEnabled()) {
      LOG.info("Creating reader for " + path + " (" + split.getPath() + ")");
    }
    long startTime = counters.startTimeCounter();
    ReaderOptions opts = OrcFile.readerOptions(conf).filesystem(fs).fileMetadata(fileMetadata);
    orcReader = EncodedOrcFile.createReader(path, opts);
    counters.incrTimeCounter(Counter.HDFS_TIME_US, startTime);
  }

  /**
   *  Gets file metadata for the split from cache, or reads it from the file.
   */
  private OrcFileMetadata getOrReadFileMetadata() throws IOException {
    OrcFileMetadata metadata = null;
    if (fileId != null) {
      metadata = metadataCache.getFileMetadata(fileId);
      if (metadata != null) {
        counters.incrCounter(Counter.METADATA_CACHE_HIT);
        return metadata;
      }
      counters.incrCounter(Counter.METADATA_CACHE_MISS);
    }
    ensureOrcReader();
    // We assume this call doesn't touch HDFS because everything is already read; don't add time.
    metadata = new OrcFileMetadata(fileId != null ? fileId : 0, orcReader);
    return (fileId == null) ? metadata : metadataCache.putFileMetadata(metadata);
  }

  /**
   * Reads the metadata for all stripes in the file.
   */
  private ArrayList<OrcStripeMetadata> readStripesMetadata(
      boolean[] globalInc, boolean[] sargColumns) throws IOException {
    ArrayList<OrcStripeMetadata> result = new ArrayList<OrcStripeMetadata>(readState.length);
    boolean hasFileId = this.fileId != null;
    long fileId = hasFileId ? this.fileId : 0;
    OrcBatchKey stripeKey = hasFileId ? new OrcBatchKey(fileId, 0, 0) : null;
    for (int stripeIxMod = 0; stripeIxMod < readState.length; ++stripeIxMod) {
      OrcStripeMetadata value = null;
      int stripeIx = stripeIxMod + stripeIxFrom;
      if (hasFileId) {
        stripeKey.stripeIx = stripeIx;
        value = metadataCache.getStripeMetadata(stripeKey);
      }
      if (value == null || !value.hasAllIndexes(globalInc)) {
        counters.incrCounter(Counter.METADATA_CACHE_MISS);
        ensureMetadataReader();
        StripeInformation si = fileMetadata.getStripes().get(stripeIx);
        if (value == null) {
          long startTime = counters.startTimeCounter();
          value = new OrcStripeMetadata(stripeKey, metadataReader, si, globalInc, sargColumns);
          counters.incrTimeCounter(Counter.HDFS_TIME_US, startTime);
          if (hasFileId) {
            value = metadataCache.putStripeMetadata(value);
            if (DebugUtils.isTraceOrcEnabled()) {
              LlapIoImpl.LOG.info("Caching stripe " + stripeKey.stripeIx
                  + " metadata with includes: " + DebugUtils.toString(globalInc));
            }
            // Create new key object to reuse for gets; we've used the old one to put in cache.
            stripeKey = new OrcBatchKey(fileId, 0, 0);
          }
        }
        // We might have got an old value from cache; recheck it has indexes.
        if (!value.hasAllIndexes(globalInc)) {
          if (DebugUtils.isTraceOrcEnabled()) {
            LlapIoImpl.LOG.info("Updating indexes in stripe " + stripeKey.stripeIx
                + " metadata for includes: " + DebugUtils.toString(globalInc));
          }
          updateLoadedIndexes(value, si, globalInc, sargColumns);
        }
      } else {
        counters.incrCounter(Counter.METADATA_CACHE_HIT);
      }
      result.add(value);
      consumer.setStripeMetadata(value);
    }
    return result;
  }

  private void ensureMetadataReader() throws IOException {
    ensureOrcReader();
    if (metadataReader != null) return;
    long startTime = counters.startTimeCounter();
    metadataReader = orcReader.metadata();
    counters.incrTimeCounter(Counter.HDFS_TIME_US, startTime);
  }

  @Override
  public void returnData(OrcEncodedColumnBatch ecb) {
    for (ColumnStreamData[] datas : ecb.getColumnData()) {
      if (datas == null) continue;
      for (ColumnStreamData data : datas) {
        if (data == null || data.decRef() != 0) continue;
        if (DebugUtils.isTraceLockingEnabled()) {
          for (MemoryBuffer buf : data.getCacheBuffers()) {
            LlapIoImpl.LOG.info("Unlocking " + buf + " at the end of processing");
          }
        }
        lowLevelCache.releaseBuffers(data.getCacheBuffers());
        CSD_POOL.offer(data);
      }
    }
    // We can offer ECB even with some streams not discarded; reset() will clear the arrays.
    ECB_POOL.offer(ecb);
  }

  /**
   * Determines which RGs need to be read, after stripes have been determined.
   * SARG is applied, and readState is populated for each stripe accordingly.
   */
  private boolean determineRgsToRead(boolean[] globalIncludes, int rowIndexStride,
      ArrayList<OrcStripeMetadata> metadata) throws IOException {
    SargApplier sargApp = null;
    if (sarg != null && rowIndexStride != 0) {
      List<OrcProto.Type> types = fileMetadata.getTypes();
      String[] colNamesForSarg = OrcInputFormat.getSargColumnNames(
          columnNames, types, globalIncludes, fileMetadata.isOriginalFormat());
      sargApp = new SargApplier(sarg, colNamesForSarg, rowIndexStride, types, globalIncludes.length);
    }
    boolean hasAnyData = false;
    // readState should have been initialized by this time with an empty array.
    for (int stripeIxMod = 0; stripeIxMod < readState.length; ++stripeIxMod) {
      int stripeIx = stripeIxMod + stripeIxFrom;
      StripeInformation stripe = fileMetadata.getStripes().get(stripeIx);
      int rgCount = getRgCount(stripe, rowIndexStride);
      boolean[] rgsToRead = null;
      if (sargApp != null) {
        OrcStripeMetadata stripeMetadata = metadata.get(stripeIxMod);
        rgsToRead = sargApp.pickRowGroups(stripe, stripeMetadata.getRowIndexes(),
            stripeMetadata.getBloomFilterIndexes(), true);
      }
      boolean isNone = rgsToRead == SargApplier.READ_NO_RGS,
          isAll = rgsToRead == SargApplier.READ_ALL_RGS;
      hasAnyData = hasAnyData || !isNone;
      if (DebugUtils.isTraceOrcEnabled()) {
        if (isNone) {
          LlapIoImpl.LOG.info("SARG eliminated all RGs for stripe " + stripeIx);
        } else if (!isAll) {
          LlapIoImpl.LOG.info("SARG picked RGs for stripe " + stripeIx + ": "
              + DebugUtils.toString(rgsToRead));
        } else {
          LlapIoImpl.LOG.info("Will read all " + rgCount + " RGs for stripe " + stripeIx);
        }
      }
      assert isAll || isNone || rgsToRead.length == rgCount;
      readState[stripeIxMod] = new boolean[columnIds.size()][];
      for (int j = 0; j < columnIds.size(); ++j) {
        readState[stripeIxMod][j] = (isAll || isNone) ? rgsToRead :
          Arrays.copyOf(rgsToRead, rgsToRead.length);
      }

      adjustRgMetric(rgCount, rgsToRead, isNone, isAll);
    }
    return hasAnyData;
  }

  private void adjustRgMetric(int rgCount, boolean[] rgsToRead, boolean isNone,
      boolean isAll) {
    int count = 0;
    if (!isAll) {
      for (boolean b : rgsToRead) {
        if (b)
          count++;
      }
    } else if (!isNone) {
      count = rgCount;
    }
    counters.setCounter(QueryFragmentCounters.Counter.SELECTED_ROWGROUPS, count);
  }


  private int getRgCount(StripeInformation stripe, int rowIndexStride) {
    return (int)Math.ceil((double)stripe.getNumberOfRows() / rowIndexStride);
  }

  /**
   * Determine which stripes to read for a split. Populates stripeIxFrom and readState.
   */
  public void determineStripesToRead() {
    // The unit of caching for ORC is (rg x column) (see OrcBatchKey).
    List<StripeInformation> stripes = fileMetadata.getStripes();
    long offset = split.getStart(), maxOffset = offset + split.getLength();
    stripeIxFrom = -1;
    int stripeIxTo = -1;
    if (LlapIoImpl.LOGL.isDebugEnabled()) {
      String tmp = "FileSplit {" + split.getStart() + ", " + split.getLength() + "}; stripes ";
      for (StripeInformation stripe : stripes) {
        tmp += "{" + stripe.getOffset() + ", " + stripe.getLength() + "}, ";
      }
      LlapIoImpl.LOG.debug(tmp);
    }

    int stripeIx = 0;
    for (StripeInformation stripe : stripes) {
      long stripeStart = stripe.getOffset();
      if (offset > stripeStart) {
        // We assume splits will never start in the middle of the stripe.
        ++stripeIx;
        continue;
      }
      if (stripeIxFrom == -1) {
        if (DebugUtils.isTraceOrcEnabled()) {
          LlapIoImpl.LOG.info("Including stripes from " + stripeIx
              + " (" + stripeStart + " >= " + offset + ")");
        }
        stripeIxFrom = stripeIx;
      }
      if (stripeStart >= maxOffset) {
        stripeIxTo = stripeIx;
        if (DebugUtils.isTraceOrcEnabled()) {
          LlapIoImpl.LOG.info("Including stripes until " + stripeIxTo + " (" + stripeStart
              + " >= " + maxOffset + "); " + (stripeIxTo - stripeIxFrom) + " stripes");
        }
        break;
      }
      ++stripeIx;
    }
    if (stripeIxFrom == -1) {
      if (LlapIoImpl.LOG.isInfoEnabled()) {
        LlapIoImpl.LOG.info("Not including any stripes - empty split");
      }
    }
    if (stripeIxTo == -1 && stripeIxFrom != -1) {
      stripeIxTo = stripeIx;
      if (DebugUtils.isTraceOrcEnabled()) {
        LlapIoImpl.LOG.info("Including stripes until " + stripeIx + " (end of file); "
            + (stripeIxTo - stripeIxFrom) + " stripes");
      }
    }
    readState = new boolean[stripeIxTo - stripeIxFrom][][];
  }

  // TODO: split by stripe? we do everything by stripe, and it might be faster
  /**
   * Takes the data from high-level cache for all stripes and returns to consumer.
   * @return List of columns to read per stripe, if any columns were fully eliminated by cache.
   */
  private List<Integer>[] produceDataFromCache(int rowIndexStride) throws IOException {
    OrcCacheKey key = new OrcCacheKey(fileId, -1, -1, -1);
    // For each stripe, keep a list of columns that are not fully in cache (null => all of them).
    @SuppressWarnings("unchecked")
    List<Integer>[] stripeColsNotInCache = new List[readState.length];
    for (int stripeIxMod = 0; stripeIxMod < readState.length; ++stripeIxMod) {
      key.stripeIx = stripeIxFrom + stripeIxMod;
      boolean[][] cols = readState[stripeIxMod];
      boolean[] isMissingAnyRgs = new boolean[cols.length];
      int totalRgCount = getRgCount(fileMetadata.getStripes().get(key.stripeIx), rowIndexStride);
      for (int rgIx = 0; rgIx < totalRgCount; ++rgIx) {
        OrcEncodedColumnBatch col = ECB_POOL.take();
        col.init(fileId, key.stripeIx, rgIx, cols.length);
        boolean hasAnyCached = false;
        try {
          key.rgIx = rgIx;
          for (int colIxMod = 0; colIxMod < cols.length; ++colIxMod) {
            boolean[] readMask = cols[colIxMod];
            // Check if RG is eliminated by SARG
            if ((readMask == SargApplier.READ_NO_RGS) || (readMask != SargApplier.READ_ALL_RGS
                && (readMask.length <= rgIx || !readMask[rgIx]))) continue;
            key.colIx = columnIds.get(colIxMod);
            ColumnStreamData[] cached = cache.get(key);
            if (cached == null) {
              isMissingAnyRgs[colIxMod] = true;
              continue;
            }
            assert cached.length == OrcEncodedColumnBatch.MAX_DATA_STREAMS;
            col.setAllStreamsData(colIxMod, key.colIx, cached);
            hasAnyCached = true;
            if (readMask == SargApplier.READ_ALL_RGS) {
              // We were going to read all RGs, but some were in cache, allocate the mask.
              cols[colIxMod] = readMask = new boolean[totalRgCount];
              Arrays.fill(readMask, true);
            }
            readMask[rgIx] = false; // Got from cache, don't read from disk.
          }
        } catch (Throwable t) {
          // TODO: Any cleanup needed to release data in col back to cache should be here.
          throw (t instanceof IOException) ? (IOException)t : new IOException(t);
        }
        if (hasAnyCached) {
          consumer.consumeData(col);
        }
      }
      boolean makeStripeColList = false; // By default assume we'll fetch all original columns.
      for (int colIxMod = 0; colIxMod < cols.length; ++colIxMod) {
        if (isMissingAnyRgs[colIxMod]) {
          if (makeStripeColList) {
            stripeColsNotInCache[stripeIxMod].add(columnIds.get(colIxMod));
          }
        } else if (!makeStripeColList) {
          // Some columns were fully in cache. Make a per-stripe col list, add previous columns.
          makeStripeColList = true;
          stripeColsNotInCache[stripeIxMod] = new ArrayList<Integer>(cols.length - 1);
          for (int i = 0; i < colIxMod; ++i) {
            stripeColsNotInCache[stripeIxMod].add(columnIds.get(i));
          }
        }
      }
    }
    return stripeColsNotInCache;
  }

  @Override
  public void setDone() {
    consumer.setDone();
  }

  @Override
  public void consumeData(OrcEncodedColumnBatch data) {
    // Store object in cache; create new key object - cannot be reused.
    assert cache != null;
    throw new UnsupportedOperationException("not implemented");
    /*for (int i = 0; i < data.getColumnData().length; ++i) {
      OrcCacheKey key = new OrcCacheKey(data.getBatchKey(), data.getColumnIxs()[i]);
      ColumnStreamData[] toCache = data.getColumnData()[i];
      ColumnStreamData[] cached = cache.cacheOrGet(key, toCache);
      if (toCache != cached) {
        for (ColumnStreamData sb : toCache) {
          if (sb.decRef() != 0) continue;
          lowLevelCache.releaseBuffers(sb.getCacheBuffers());
        }
        data.getColumnData()[i] = cached;
      }
    }
    consumer.consumeData(data);*/
  }

  @Override
  public void setError(Throwable t) {
    consumer.setError(t);
  }

  private class DataWrapperForOrc implements DataReader, DataCache {
    private final DataReader orcDataReader;

    public DataWrapperForOrc() {
      boolean useZeroCopy = (conf != null) && OrcConf.USE_ZEROCOPY.getBoolean(conf);
      if (useZeroCopy && !lowLevelCache.getAllocator().isDirectAlloc()) {
        throw new UnsupportedOperationException("Cannot use zero-copy reader with non-direct cache "
            + "buffers; either disable zero-copy or enable direct cache allocation");
      }
      this.orcDataReader = orcReader.createDefaultDataReader(useZeroCopy);
    }

    @Override
    public DiskRangeList getFileData(long fileId, DiskRangeList range,
        long baseOffset, DiskRangeListFactory factory, BooleanRef gotAllData) {
      return lowLevelCache.getFileData(fileId, range, baseOffset, factory, counters, gotAllData);
    }

    @Override
    public long[] putFileData(long fileId, DiskRange[] ranges,
        MemoryBuffer[] data, long baseOffset) {
      return lowLevelCache.putFileData(
          fileId, ranges, data, baseOffset, Priority.NORMAL, counters);
    }

    @Override
    public void releaseBuffer(MemoryBuffer buffer) {
      lowLevelCache.releaseBuffer(buffer);
    }

    @Override
    public void reuseBuffer(MemoryBuffer buffer) {
      boolean isReused = lowLevelCache.reuseBuffer(buffer);
      assert isReused;
    }

    @Override
    public Allocator getAllocator() {
      return lowLevelCache.getAllocator();
    }

    @Override
    public void close() throws IOException {
      orcDataReader.close();
    }

    @Override
    public DiskRangeList readFileData(DiskRangeList range, long baseOffset,
        boolean doForceDirect) throws IOException {
      long startTime = counters.startTimeCounter();
      DiskRangeList result = orcDataReader.readFileData(range, baseOffset, doForceDirect);
      counters.recordHdfsTime(startTime);
      if (DebugUtils.isTraceOrcEnabled() && LOG.isInfoEnabled()) {
        LOG.info("Disk ranges after disk read (file " + fileId + ", base offset " + baseOffset
              + "): " + RecordReaderUtils.stringifyDiskRanges(result));
      }
      return result;
    }

    @Override
    public boolean isTrackingDiskRanges() {
      return orcDataReader.isTrackingDiskRanges();
    }

    @Override
    public void releaseBuffer(ByteBuffer buffer) {
      orcDataReader.releaseBuffer(buffer);
    }

    @Override
    public void open() throws IOException {
      long startTime = counters.startTimeCounter();
      orcDataReader.open();
      counters.recordHdfsTime(startTime);
    }
  }
}
